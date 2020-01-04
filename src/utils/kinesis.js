import _ from 'highland';

import Publisher from '../connectors/kinesis';

import { skipTag } from '../filters';
import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const publishEvents = ({
  debug = d('kinesis'),
  streamName = process.env.STREAM_NAME,
  eventField = 'event',
} = {}) => {
  const connector = new Publisher({ debug, streamName });

  return (batchUow) => {
    batchUow = adornStandardTags(batchUow, eventField);

    const p = connector.publish(batchUow.batch.map((uow) => uow[eventField]))
      .then((publishResponse) => ({ ...batchUow, publishResponse }))
      .catch(rejectWithFault(batchUow));

    return _(p);
  };
};

export const adornStandardTags = (batchUow, eventField) => ({
  batch: batchUow.batch.map((uow) => ({
    ...uow,
    event: {
      ...uow[eventField],
      tags: {
        ...envTags(uow),
        ...skipTag(),
        ...uow[eventField].tags,
      },
    },
  })),
});

export const envTags = (uow) => ({
  account: process.env.ACCOUNT_NAME || 'undefined',
  region: process.env.AWS_REGION || /* istanbul ignore next */ 'undefined',
  stage: process.env.SERVERLESS_STAGE || 'undefined',
  source: process.env.SERVERLESS_PROJECT || 'undefined',
  functionname: process.env.AWS_LAMBDA_FUNCTION_NAME || 'undefined',
  pipeline: uow.pipeline || 'undefined',
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
