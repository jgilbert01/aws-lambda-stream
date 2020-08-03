import omit from 'lodash/omit';

import {
  printStartPipeline, printEndPipeline,
  faulty,
  ttl,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip,
} from '../filters';

import { put } from '../utils/dynamodb';

// collects events in a micro event store
// provides idempotency
// the partitionKey is used as the default correlation key
// uses the DynamoDB single table pattern

export const collect = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toPutRequest(rule))

  .through(put(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toPutRequest = (rule) => faulty(
  (uow) => ({
    ...uow,
    putRequest: { // variable expected by `put` util
      Item: {
        pk: uow.event.id,
        sk: 'EVENT',
        discriminator: 'EVENT',
        timestamp: uow.event.timestamp,
        sequenceNumber: uow.record.kinesis.sequenceNumber,
        ttl: ttl(uow.event.timestamp, rule.ttl || process.env.TTL || 11), // days
        data: uow.event.partitionKey, // TODO make configurable
        event: rule.includeRaw ? /* istanbul ignore next */ uow.event : omit(uow.event, ['raw']),
      },
    },
  }),
);
