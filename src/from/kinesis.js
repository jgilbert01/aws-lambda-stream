import _ from 'highland';

import { faulty } from '../utils';

export const fromKinesis = (event) =>

  _(event.Records)

    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: Buffer.from(record.kinesis.data, 'base64').toString('utf8'),
      }))

    .map(faulty((uow) => ({
      ...uow,
      event: {
        id: uow.record.eventID,
        ...JSON.parse(uow.event),
      },
    })));

// test helper
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
