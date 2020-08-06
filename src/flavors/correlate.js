import get from 'lodash/get';
import isFunction from 'lodash/isFunction';

import {
  printStartPipeline, printEndPipeline,
  faulty,
  ttlRule,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
} from '../filters';

import { put } from '../utils/dynamodb';

/**
 * used when you need to correlate the same events under multiple keys
 * used in trigger functions
 * inserts event back into the micro event store under different correlation keys
 * uses the DynamoDB single table pattern
 *
 * interface Rule {
 *   id: string
 *   flavor: correlate,
 *   eventType: string | string[] | Function,
 *   filters: Function[],
 *   correlationKey: string | Function,
 *   correlationKeySuffix?: string,
 *   ttl?: number, // default ttl of collected event
 *   expire: boolean | string
 *   parallel?: number;
 * }
 */

export const correlate = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(forCollectedEvents)
  .map(normalize)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(correlationKey(rule))
  .map(toPutRequest(rule))
  .through(put(rule))

  .tap(printEndPipeline);

const forCollectedEvents = (uow) => (uow.record.eventName === 'INSERT' && uow.record.dynamodb.Keys.sk.S === 'EVENT');
const normalize = (uow) => ({
  ...uow,
  meta: {
    sequenceNumber: uow.event.raw.new.sequenceNumber,
    ttl: uow.event.raw.new.ttl,
    data: uow.event.raw.new.data,
  },
  event: uow.event.raw.new.event,
});

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const correlationKey = (rule) => faulty((uow) => {
  let key;

  if (isFunction(rule.correlationKey)) {
    key = rule.correlationKey(uow);
  } else {
    key = get(uow.event, rule.correlationKey);
  }

  // use a suffix when you need the same key for different sets of rules
  key = rule.correlationKeySuffix ? `${key}.${rule.correlationKeySuffix}` : key;

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
        pk: uow.key,
        sk: uow.event.id,
        discriminator: 'CORREL', // ATION
        timestamp: uow.event.timestamp,
        sequenceNumber: uow.meta.sequenceNumber,
        ttl: rule.ttl ? ttlRule(rule, uow) : uow.meta.ttl,
        expire: rule.expire,
        suffix: rule.correlationKeySuffix,
        ruleId: rule.id,
        event: uow.event,
      },
    },
  }),
);
