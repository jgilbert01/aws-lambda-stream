import _ from 'highland';

import Connector from '../connectors/sqs';

import { toBatchUow, unBatchUow } from './batch';
import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const sendToSqs = ({ // eslint-disable-line import/prefer-default-export
  debug = d('sqs'),
  streamName: queueName = process.env.QUEUE_NAME,
  messageField = 'message',
  batchSize = Number(process.env.SQS_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.SQS_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, queueName });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Entries: batchUow.batch
        .map((uow) => uow[messageField]),
    },
  });

  const sendMessageBatch = (batchUow) => {
    const p = connector.sendMessageBatch(batchUow.inputParams)
      .then((sendMessageBatchResponse) => ({ ...batchUow, sendMessageBatchResponse }))
      .catch(rejectWithFault(batchUow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s

    .batch(batchSize)
    .map(toBatchUow)

    .map(toInputParams)
    .map(sendMessageBatch)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};
