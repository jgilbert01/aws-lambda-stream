import _ from 'highland';

import {
  faulty, decompress, compress,
} from '../utils';
import { outSkip } from '../filters';
import { redeemClaimcheck } from '../queries';

export const fromFirehose = (event) =>

  _(event.records)
    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: Buffer.from(record.data, 'base64').toString('utf8'),
        recordId: record.recordId,
        result: 'Dropped', // by default, set to 'Ok' in transform, 'ProcessingFailed' in fault processing ???
      }))

    .map(faulty((uow) => ({
      ...uow,
      event: JSON.parse(uow.event, decompress),
    })))
    .filter(outSkip)
    .through(redeemClaimcheck());

// test helper
export const toFirehoseRecords = (events, approximateArrivalTimestamp) => ({
  invocationId: 'invocationIdExample',
  deliveryStreamArn: 'arn:aws:kinesis:TEST',
  region: process.env.AWS_REGION || /* istanbul ignore next */ 'us-west-2',
  records: events.map((e, i) =>
    ({
      recordId: `${i}`, // "49546986683135544286507457936321625675700192471156785154",
      data: Buffer.from(JSON.stringify(e, compress())).toString('base64'),
      approximateArrivalTimestamp, // format: 1495072949453
    })),
});

export const UNKNOWN_FIREHOSE_EVENT_TYPE = toFirehoseRecords([{ type: 'unknown-type' }]);
