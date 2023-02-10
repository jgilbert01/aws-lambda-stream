import _ from 'highland';

import Publisher from '../connectors/kinesis';

import { toBatchUow, unBatchUow } from './batch';
import { rejectWithFault } from './faults';
import { debug as d } from './print';
import { adornStandardTags } from './tags';
import { compress } from './compression';

export const publishToKinesis = ({
  debug = d('kinesis'),
  streamName = process.env.STREAM_NAME,
  eventField = 'event',
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  ...opt
} = {}) => {
  const connector = new Publisher({ debug, streamName });

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

    const p = connector.putRecords(batchUow.inputParams)
      .then((publishResponse) => ({ ...batchUow, publishResponse }))
      .catch(rejectWithFault(batchUow, !handleErrors));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(adornStandardTags(eventField))

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
