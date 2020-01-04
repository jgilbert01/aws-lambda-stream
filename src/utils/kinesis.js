import _ from 'highland';

import Publisher from '../connectors/kinesis';

import { skipTag } from '../filters';
import { toBatchUow, unBatchUow } from './batch';
import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const publish = ({
  debug = d('kinesis'),
  streamName = process.env.STREAM_NAME,
  eventField = 'event',
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
} = {}) => {
  const connector = new Publisher({ debug, streamName });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Records: batchUow.batch
        .map((uow) => toRecord(uow[eventField])),
    },
  });

  const putRecords = (batchUow) => {
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

export const toRecord = (e) => ({
  Data: Buffer.from(JSON.stringify(e)),
  PartitionKey: e.partitionKey,
});

export const adornStandardTags = (eventField) => (uow) => ({
  ...uow,
  event: {
    ...uow[eventField],
    tags: {
      ...envTags(uow.pipeline),
      ...skipTag(),
      ...uow[eventField].tags,
    },
  },
});

export const envTags = (pipeline) => ({
  account: process.env.ACCOUNT_NAME || 'undefined',
  region: process.env.AWS_REGION || /* istanbul ignore next */ 'undefined',
  stage: process.env.SERVERLESS_STAGE || 'undefined',
  source: process.env.SERVERLESS_PROJECT || 'undefined',
  functionname: process.env.AWS_LAMBDA_FUNCTION_NAME || 'undefined',
  pipeline: pipeline || 'undefined',
});

// testing
export const toKinesisRecords = (events) => ({
  Records: events.map((e, i) =>
    ({
      eventSource: 'aws:kinesis',
      // eventVersion: '1.0',
      eventID: `shardId-000000000000:${i}`,
      // eventName: 'aws:kinesis:record',
      // invokeIdentityArn: 'arn:aws:iam::123456789012:role/lambda-role',
      awsRegion: 'us-west-2',
      // eventSourceARN: 'arn:aws:kinesis:us-west-2:123456789012:stream/lambda-stream',
      kinesis: {
        // kinesisSchemaVersion: '1.0',
        // partitionKey: e.partitionKey,
        sequenceNumber: `${i}`,
        data: Buffer.from(JSON.stringify(e)).toString('base64'),
        // approximateArrivalTimestamp: 1545084650.987,
      },
    })),
});
