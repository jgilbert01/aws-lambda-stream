import _ from 'highland';

import Publisher from '../connectors/kinesis';

import { rejectWithFault } from './faults';

export const publishEvents = (debug, streamName = process.env.STREAM_NAME, eventField = 'event') => (batchUow) => {
  const connector = new Publisher(debug, streamName);
  const p = connector.publish(batchUow.batch.map((uow) => uow[eventField]))
    .then((publishResponse) => ({ ...batchUow, publishResponse }))
    .catch(rejectWithFault(batchUow));

  return _(p);
};

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
