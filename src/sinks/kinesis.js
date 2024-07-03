import _ from 'highland';

import Publisher from '../connectors/kinesis';

import { toBatchUow, unBatchUow } from '../utils/batch';
import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { adornStandardTags } from '../utils/tags';
import { compress } from '../utils/compression';
import { ratelimit } from '../utils/ratelimit';

export const publishToKinesis = ({
  id: pipelineId,
  debug = d('kinesis'),
  streamName = process.env.STREAM_NAME,
  eventField = 'event',
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  step = 'publish',
  ...opt
} = {}) => {
  const connector = new Publisher({
    pipelineId, debug, streamName, ...opt,
  });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Records: batchUow.batch
        .filter((uow) => uow[eventField])
        .map((uow) => toRecord(uow[eventField], opt)),
    },
  });

  const putRecords = (batchUow) => {
    if (!batchUow.inputParams.Records.length) {
      return _(Promise.resolve(batchUow));
    }

    const p = () => connector.putRecords(batchUow.inputParams, batchUow)
      .then((publishResponse) => ({ ...batchUow, publishResponse }))
      .catch(rejectWithFault(batchUow, !handleErrors));

    return _(batchUow.batch[0].metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(adornStandardTags(eventField))

    .through(ratelimit(opt))

    .batch(batchSize)
    .map(toBatchUow)

    .map(toInputParams)
    .map(putRecords)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};

export const toRecord = (e, opt) => ({
  Data: Buffer.from(JSON.stringify(e, compress(opt))),
  PartitionKey: e.partitionKey,
});
