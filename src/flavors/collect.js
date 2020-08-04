import get from 'lodash/get';
import isFunction from 'lodash/isFunction';
import omit from 'lodash/omit';

import {
  printStartPipeline, printEndPipeline,
  faulty,
  ttlRule,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip,
} from '../filters';

import { put } from '../utils/dynamodb';

/**
 * collects events in a micro event store
 * used in listener functions
 * provides idempotency
 * the partitionKey is used as the default correlation key
 * uses the DynamoDB single table pattern
 *
 * interface Rule {
 *   id: string
 *   flavor: collect,
 *   eventType: string | string[] | Function,
 *   filters: Function[],
 *   correlationKey?: string | Function, // default uow.event.partitionKey
 *   ttl?: number, // default 33 days
 *   expire: boolean | string
 * }
 */

export const collect = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(correlationKey(rule))
  .map(toPutRequest(rule))
  .through(put(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const correlationKey = (rule) => faulty((uow) => {
  let key;

  if (!rule.correlationKey) {
    key = uow.event.partitionKey;
  } else if (isFunction(rule.correlationKey)) {
    key = rule.correlationKey(uow);
  } else {
    key = get(uow.event, rule.correlationKey);
  }

  return ({
    ...uow,
    key,
  });
});

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
        ttl: ttlRule(rule, uow),
        expire: rule.expire,
        data: uow.key,
        event: rule.includeRaw ? /* istanbul ignore next */ uow.event : omit(uow.event, ['raw']),
      },
    },
  }),
);
