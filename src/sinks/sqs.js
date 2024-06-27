import _ from 'highland';

import Connector from '../connectors/sqs';

import { toBatchUow, unBatchUow } from '../utils/batch';
import { ratelimit } from '../utils/ratelimit';
import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';

export const sendToSqs = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('sqs'),
  queueUrl = process.env.QUEUE_URL,
  messageField = 'message',
  batchSize = Number(process.env.SQS_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 10,
  parallel = Number(process.env.SQS_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, queueUrl, ...opt,
  });

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

    const p = connector.sendMessageBatch(batchUow.inputParams, batchUow)
      .then((sendMessageBatchResponse) => ({ ...batchUow, sendMessageBatchResponse }))
      .catch(rejectWithFault(batchUow));

    return _(batchUow.batch[0].metrics?.w(p, 'send') || p); // wrap promise in a stream
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
