import _ from 'highland';

import {
  faulty, decompress, compress, claimcheck,
} from '../utils';
import { outSkip } from '../filters';

import * as metrics from '../metrics';

export const fromKinesis = (event) =>

  _(event.Records)

    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: Buffer.from(record.kinesis.data, 'base64').toString('utf8'),
        metrics: metrics.startUow(metrics.convertKinesisTs(record.kinesis.approximateArrivalTimestamp), event.Records.length),
      }))

    .map(faulty((uow) => ({
      ...uow,
      event: {
        id: uow.record.eventID,
        ...JSON.parse(uow.event, decompress),
      },
    })))
    .filter(outSkip)
    .through(claimcheck());

// test helper
export const toKinesisRecords = (events, approximateArrivalTimestamp) => ({
  Records: events.map((e, i) =>
    ({
      eventSource: 'aws:kinesis',
      // eventVersion: '1.0',
      eventID: `shardId-000000000000:${i}`,
      // eventName: 'aws:kinesis:record',
      // invokeIdentityArn: 'arn:aws:iam::123456789012:role/lambda-role',
      awsRegion: process.env.AWS_REGION || /* istanbul ignore next */ 'us-west-2',
      // eventSourceARN: 'arn:aws:kinesis:us-west-2:123456789012:stream/lambda-stream',
      kinesis: {
        // kinesisSchemaVersion: '1.0',
        // partitionKey: e.partitionKey,
        sequenceNumber: `${i}`,
        data: Buffer.from(JSON.stringify(e, compress())).toString('base64'),
        approximateArrivalTimestamp, // format: 1545084650.987
      },
    })),
});

export const UNKNOWN_KINESIS_EVENT_TYPE = toKinesisRecords([{ type: 'unknown-type' }]);
