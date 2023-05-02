import _ from 'highland';

import Connector from '../connectors/sqs';

import { toBatchUow, unBatchUow } from './batch';
import { ratelimit } from './ratelimit';
import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const sendToSqs = ({ // eslint-disable-line import/prefer-default-export
  debug = d('sqs'),
  queueUrl = process.env.QUEUE_URL,
  messageField = 'message',
  batchSize = Number(process.env.SQS_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 10,
  parallel = Number(process.env.SQS_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, queueUrl });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Entries: batchUow.batch
        .filter((uow) => uow[messageField])
        .map((uow) => uow[messageField]),
    },
  });

  const sendMessageBatch = (batchUow) => {
    /* istanbul ignore next */
    if (!batchUow.inputParams.Entries.length) {
      return _(Promise.resolve(batchUow));
    }

    const p = connector.sendMessageBatch(batchUow.inputParams)
      .then((sendMessageBatchResponse) => ({ ...batchUow, sendMessageBatchResponse }))
      .catch(rejectWithFault(batchUow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))

    .batch(batchSize)
    .map(toBatchUow)

    .map(toInputParams)
    .map(sendMessageBatch)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};
