import _ from 'highland';
import isFunction from 'lodash/isFunction';
import { get } from 'lodash';
import { toClaimcheckEvent, toPutClaimcheckRequest } from '../sinks/claimcheck';

// used after highland batch step
export const toBatchUow = (batch) => ({ batch });

// use with flatMap
export const unBatchUow = (uow) => {
  const { batch, ...outerUowMinusBatch } = uow;
  return batch.map((innerUowFromBatch) => ({
    ...innerUowFromBatch,
    ...outerUowMinusBatch,
  }));
};

export const group = (rule) => {
  /* istanbul ignore if */
  if (!rule.group) return (s) => s;

  return (s) => s
    .group((uow) => uow.event.partitionKey)
    .flatMap(toGroupUows);
};

// use with flatMap after highland group step
export const toGroupUows = (groups) => _(Object.keys(groups).map((key) => ({ batch: groups[key] })));

export const compact = (rule) => {
  if (!rule.compact) return (s) => s;

  return (s) => (isFunction(rule.compact) && /* istanbul ignore next */ rule.compact(s)) || s
    .group(rule.compact.group || ((uow) => uow.event.partitionKey))
    .flatMap((groups) => _(Object.keys(groups)
      .map((key) => {
        const batch = groups[key].sort(rule.compact.sort || ((lh, rh) => lh.event.timestamp - rh.event.timestamp));
        const last = batch[batch.length - 1];

        const metrics = last.metrics?.opt.metrics.enabled('compact') ? {
          metrics: last.metrics.gauge('stream.pipeline.compact.count', batch.length),
        } : {};

        return {
          ...last,
          batch,
          ...metrics,
        };
      })));
};

/**
 * Batch EB request entries by size to avoid writing a batch that's too large to EB.
 */
export const batchWithSize = ({
  claimCheckBucketName = process.env.CLAIMCHECK_BUCKET_NAME,
  putClaimcheckRequest = 'putClaimcheckRequest',
  ...opt
}) => {
  let batched = [];
  let sizes = [];

  return (err, x, push, next) => {
    /* istanbul ignore if */
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      if (batched.length > 0) {
        logMetrics(batched, sizes, opt);
        push(null, batched);
      }

      push(null, _.nil);
    } else {
      if (!x[opt.requestEntryField]) {
        push(null, [x]);
      } else {
        let size = Buffer.byteLength(JSON.stringify(x[opt.requestEntryField]));
        if (size > opt.maxRequestSize) {
          logMetrics([x], [size], opt);
          if (claimCheckBucketName) {
            // setup claim check
            x[putClaimcheckRequest] = toPutClaimcheckRequest(JSON.parse(x[opt.requestEntryField].Detail), claimCheckBucketName);
            x[opt.requestEntryField].Detail = JSON.stringify(toClaimcheckEvent(
              JSON.parse(x[opt.requestEntryField].Detail),
              claimCheckBucketName,
            ));
            size = Buffer.byteLength(JSON.stringify(x[opt.requestEntryField]));
          } else {
            const error = new Error(`Request size: ${size}, exceeded max: ${opt.maxRequestSize}`);
            error.uow = x;
            push(error);
            next();
            return;
          }
        }

        const totalSize = sizes.reduce((a, c) => a + c, size);

        if (totalSize <= opt.maxRequestSize && batched.length + 1 <= opt.batchSize) {
          batched.push(x);
          sizes.push(size);
        } else {
          logMetrics(batched, sizes, opt);
          push(null, batched);
          batched = [x];
          sizes = [size];
        }
      }

      next();
    }
  };
};

const logMetrics = (batch, sizes, opt) => {
  if (opt.metrics?.enabled('size')) {
    batch[0].metrics?.gauge('publish|stream.pipeline.batchSize.count', batch.length);
    batch[0].metrics?.gauge('publish|stream.pipeline.eventSize.bytes', sizes);
  }
};

/**
 * Batches by aggregate payload size with a cap on payload count.
 */
export const batchWithPayloadSizeOrCount = ({
  batchSize,
  maxPayloadSize,
  payloadField,
  ...opt
}) => {
  let batched = [];
  let sizes = [];

  return (err, x, push, next) => {
    /* istanbul ignore if */
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      if (batched.length > 0) {
        logMetrics(batched, sizes, opt);
        push(null, batched);
      }

      push(null, _.nil);
    } else {
      if (!get(x, payloadField)) {
        push(null, [x]);
      } else {
        const size = Buffer.byteLength(JSON.stringify(get(x, payloadField)));
        if (size > maxPayloadSize) {
          logMetrics([x], [size], opt);
          const error = new Error(`Payload size: ${size}, exceeded max: ${maxPayloadSize}`);
          error.uow = x;
          push(error);
          next();
          return;
        }

        const totalSize = sizes.reduce((a, c) => a + c, size);
        if (totalSize <= maxPayloadSize && batched.length + 1 <= batchSize) {
          batched.push(x);
          sizes.push(size);
        } else {
          logMetrics(batched, sizes, opt);
          push(null, batched);
          batched = [x];
          sizes = [size];
        }
      }

      next();
    }
  };
};
