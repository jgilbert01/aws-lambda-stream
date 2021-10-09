import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify, query,
  encryptEvent,
} from '../utils';

import { filterOnEventType, filterOnContent, outLatched } from '../filters';

export const cdc = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outLatched)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toQueryRequest(rule))
  .through(query(rule))

  .map(toEvent(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(encryptEvent(rule))
  .through(rule.publish(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toQueryRequest = (rule) => (uow) => ({
  ...uow,
  queryRequest: !rule.queryRelated ? undefined
    : {
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: {
        '#pk': rule.pkFn || 'pk',
      },
      ExpressionAttributeValues: {
        ':pk': uow.event.partitionKey,
      },
      ConsistentRead: true,
    },
});

const toEvent = (rule) => faultyAsyncStream(async (uow) => (!rule.toEvent
  ? uow
  : ({
    ...uow,
    event: {
      ...uow.event,
      ...await faultify(rule.toEvent)(uow, rule),
    },
  })));
