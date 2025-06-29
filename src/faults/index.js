import _ from 'highland';
import {
  now, trimAndRedact, uuid, compress, FAULT_COMPRESSION_IGNORE,
} from '../utils';

export const FAULT_EVENT_TYPE = 'fault';

// collect faults until the end so that an unhandled error
// does not cause handler faults to be repeatedly published
const theFaults = [];

export const faults = (opt) => (err, push) => {
  logErr(err);

  if (err.uow && !(opt.retryable && opt.retryable(err, opt))) {
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
        push(null, limitFaultSize(f, opt));
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
        pipeline: err.uow?.pipeline || 'undefined',
        handled: err.uow !== undefined,
        retryable: err.retryable,
        stackTrace: err.stack,
      }));
    } else {
      console.error(err);
    }
  }
};

export const limitFaultSize = (fault, opt) => {
  const str = JSON.stringify(fault, compress({ ...opt, compressionIgnore: FAULT_COMPRESSION_IGNORE }));
  const size = Buffer.byteLength(str);
  if (size > opt.maxRequestSize) {
    // just include what is essential to resubmit faults
    return {
      ...fault,
      uow: fault.uow.batch ? {
        batch: fault.uow.batch.map(({ record }) => ({ record })),
      } : {
        record: fault.uow.record,
      },
    };
    // if it is still too big there is not a lot we can do
    // maybe the original event is too big with unnecessary data
    // for a fault with uow.batch maybe reduce the batch size temporarily
    // TODO add-source-side-claim-check-support - https://github.com/jgilbert01/aws-lambda-stream/issues/355
  } else {
    return fault;
  }
};
