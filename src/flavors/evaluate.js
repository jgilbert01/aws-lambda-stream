import {
  merge, omit, last, castArray, isBoolean,
} from 'lodash';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsync,
  query,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
} from '../filters';

/**
 * used to evaluate conditions and produce higher-order events
 * used in trigger functions
 *
 * interface Rule {
 *   id: string
 *   flavor: evaluate,
 *   eventType: string | string[] | Function, // match rules to events based on event type
 *   correlationKeySuffix?: string,           // match rules to events based on correlation key suffix
 *   filters?: Function[],                    // evaluate event content
 *   expression?: Function;                   // evaluate correlated events, triggers query for correlated events
 *   emit: string | Function;                 // create higher-order event(s) to publish
 *   index?: string;
 *   batchSize?: number;
 *   parallel?: number;
 * }
 */

export const evaluate = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(forEvents)
  .map(normalize)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(complex(rule))

  .map(toHigherOrderEvents(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)
  .sequence() // may emit multiple events
  .through(rule.publish({ ...rule, eventField: 'emit' }))

  .tap(printEndPipeline);

const forEvents = (uow) => (uow.record.eventName === 'INSERT'
  && (uow.record.dynamodb.Keys.sk.S === 'EVENT' || uow.record.dynamodb.NewImage.discriminator.S === 'CORREL'));

const normalize = (uow) => ({ // we are interested in the event that was persisted, not that it was persisted
  ...uow,
  meta: {
    id: uow.event.id,
    sequenceNumber: uow.event.raw.new.sequenceNumber,
    ttl: uow.event.raw.new.ttl,
    expire: uow.event.raw.new.expire,
    pk: uow.event.raw.new.pk,
    data: uow.event.raw.new.data,
    correlationKey: uow.event.raw.new.discriminator === 'CORREL'
      ? uow.event.raw.new.pk : uow.event.raw.new.data,
    suffix: uow.event.raw.new.suffix,
    correlation: uow.event.raw.new.discriminator === 'CORREL',
  },
  event: uow.event.raw.new.event,
});

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const complex = (rule) => {
  if (!rule.expression) {
    return (s) => s
      .map((uow) => ({
        ...uow,
        triggers: [uow.event],
      }));
  } else {
    return (s) => s
      .filter(onCorrelationKeySuffix(rule))

      .map(toQueryRequest(rule))
      .through(query({ ...rule, queryResponseField: 'correlated' }))
      .map((uow) => ({
        ...uow,
        correlated: uow.correlated.map((i) => i.event),
      }))

      .map(expression(rule))
      .filter((uow) => uow.expression);
  }
};

const onCorrelationKeySuffix = (rule) => faulty((uow) => {
  // evaluate rules with no suffix against correlations with no suffix
  if (!rule.correlationKeySuffix && !uow.meta.suffix) {
    return true;
  }

  // do not evaluate rules with a suffix against correlations with no suffix
  if (rule.correlationKeySuffix && !uow.meta.suffix) {
    return false;
  }

  // evaluate rules with a suffix against correlations with the same suffix
  if (rule.correlationKeySuffix && uow.meta.suffix === rule.correlationKeySuffix) {
    return true;
  }

  // do not evaluate rules with a suffix against correlations with a different suffix
  return false;
});

const toQueryRequest = (rule) => (uow) => ({
  ...uow,
  queryRequest:
    uow.meta.correlation
      ? {
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: {
          '#pk': 'pk',
        },
        ExpressionAttributeValues: {
          ':pk': uow.meta.pk,
        },
        ConsistentRead: true,
      } : {
        IndexName: rule.index ? /* istanbul ignore next */ rule.index : 'DataIndex',
        KeyConditionExpression: '#data = :data',
        ExpressionAttributeNames: {
          '#data': 'data',
        },
        ExpressionAttributeValues: {
          ':data': uow.meta.data,
        },
        ConsistentRead: true,
        // TODO filter out expired events
      },
});

const expression = (rule) => faulty((uow) => {
  // return: Boolean | Event | Event[] | undefined
  const result = rule.expression(uow);

  return {
    ...uow,
    expression: Array.isArray(result) ? result.length > 0 : result,
    triggers: isBoolean(result) ? [uow.event] : castArray(result),
  };
});

const toHigherOrderEvents = (rule) => faultyAsync((uow) => Promise.resolve()
  .then(() => {
    const basic = (typeof rule.emit === 'string');
    const trigger = last(uow.triggers);
    const template = {
      ...(basic ? uow.event : undefined),
      id: `${uow.meta.id}.${rule.id}`, // plus a suffix if many
      type: basic ? rule.emit : undefined,
      timestamp: trigger.timestamp,
      partitionKey: uow.meta.correlationKey.replace(`.${rule.correlationKeySuffix}`, ''),
      tags: omit(uow.triggers.reduce((previous, current) =>
        merge(previous, current.tags), {}), ['region', 'source']),
      triggers: uow.triggers.map(({ id, type, timestamp }) => ({ id, type, timestamp })),
    };

    return Promise.resolve(basic
      ? template
      : rule.emit(uow, rule, template))
      .then((result) => castArray(result).map((emit) => ({
        ...uow,
        emit,
      })));
  }));
