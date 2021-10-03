import _ from 'highland';
import * as uuid from 'uuid';
import cloneDeepWith from 'lodash/cloneDeepWith';

import { now } from '../utils';

export const FAULT_EVENT_TYPE = 'fault';

// collect faults until the end so that an unhandled error
// does not cause handler faults to be repeatedly published
const theFaults = [];

export const faults = (err, push) => {
  if (err.uow) {
    // handled exceptions are adorned with the uow in error
    // push a fault event onto the stack for publishing by publishFaultsPipeline
    theFaults.push({
      id: uuid.v1(),
      partitionKey: uuid.v4(),
      type: FAULT_EVENT_TYPE,
      timestamp: now(),
      tags: {
        functionname: process.env.AWS_LAMBDA_FUNCTION_NAME || 'undefined',
        pipeline: err.uow.pipeline || 'undefined',
      },
      err: {
        name: err.name,
        message: err.message,
        stack: err.stack,
      },
      uow: trimAndRedact(err.uow),
    });

    logErr(err);
  } else {
    // rethrow unhandled/unexpected exceptions to stop processing
    push(err);
  }
};

export const flushFaults = (opt) => (s) => {
  // use at the every end with through() to redirect to faults streams

  const faultStream = () => {
    const s2 = _((push, next) => {
      const f = theFaults.shift();
      if (f) {
        push(null, f);
        next();
      } else {
        push(null, _.nil);
      }
    });

    return s2
      // batch and publish fault events
      .map((fault) => ({ event: fault })) // map to uow format
      .through(opt.publish({
        ...opt,
        ...opt.faultOpt, // override options specific for faults
        handleErrors: false, // don't publish faults for faults
        batchSize: Number(process.env.FAULTS_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 4,
        parallel: Number(process.env.FAULTS_PARALLEL) || Number(process.env.PARALLEL) || 4,
      }));
  };

  return s
    .consume((err, x, push, next) => {
      /* istanbul ignore if */
      if (err) {
        push(err);
        next();
      } else if (x === _.nil) {
        // this is the purpose of this consume step
        // publish all acuumulated faults at the very end
        next(faultStream());
      } else {
        push(null, x);
        next();
      }
    });
};

const logErr = (err) => {
  /* istanbul ignore if */
  if (process.env.AWS_LAMBDA_LOG_GROUP_NAME) {
    if (err instanceof Error) {
      console.error(JSON.stringify({
        errorMessage: err.message,
        errorType: err.name,
        pipeline: err.uow.pipeline || 'undefined',
        handled: err.uow !== undefined,
        stackTrace: err.stack,
      }));
    } else {
      console.error(err);
    }
  }
};

const trimAndRedact = (uow) => {
  const {
    pipeline, record, event, decryptResponse, undecryptedEvent, ...rest
  } = uow;
  const eem = event.eem || undecryptedEvent?.eem;
  const cache = [];

  const cloneCustomizer = (value, key) => {
    if (eem?.fields.includes(key)) {
      return '[REDACTED]';
    } else {
      if (Buffer.isBuffer(value)) {
        return `[BUFFER: ${value.length}]`;
      }

      if (typeof value === 'object' && value !== null) {
        if (cache.includes(value)) {
          return '[CIRCULAR]';
        } else {
          cache.push(value);
        }
      }
    }
    return undefined;
  };

  return {
    pipeline,
    record, // DO NOT redact so we can resubmit
    ...cloneDeepWith({
      event: undecryptedEvent || event,
      ...rest,
    }, cloneCustomizer),
  };
};
