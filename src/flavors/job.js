import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
  scanDynamoDB, queryDynamoDB, batchGetDynamoDB,
  encryptEvent,
} from '../utils';

import { filterOnEventType, filterOnContent } from '../filters';

export const job = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .map(toScanRequest(rule))
  .through(scanDynamoDB(rule))

  .filter(onContent(rule))

  .map(toQueryRequest(rule))
  .through(queryDynamoDB(rule))

  .map(toGetRequest(rule))
  .through(batchGetDynamoDB(rule))

  .map(toEvent(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(encryptEvent({
    ...rule,
    sourceField: 'emit',
    targetField: 'emit',
  }))
  .through(rule.publish({
    ...rule,
    parallel: 10,
    eventField: 'emit', // so we don't overwrite the incoming event in the uow
  }))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));

const toScanRequest = (rule) => (uow) => ({
  ...uow,
  scanRequest:
    rule.toScanRequest
      ? rule.toScanRequest(uow, rule)
      : /* istanbul ignore next */ undefined,
});

const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toQueryRequest = (rule) => (uow) => ({
  ...uow,
  queryRequest:
    rule.toQueryRequest
      ? /* istanbul ignore next */ rule.toQueryRequest(uow, rule)
      : undefined,
});

const toGetRequest = (rule) => (uow) => ({
  ...uow,
  batchGetRequest:
    rule.toGetRequest
      ? /* istanbul ignore next */ rule.toGetRequest(uow, rule)
      : undefined,
});

const toEvent = (rule) => faultyAsyncStream(async (uow) => (!rule.toEvent
  ? /* istanbul ignore next */ uow
  : ({
    ...uow,
    emit: {
      ...await faultify(rule.toEvent)(uow, rule),
    },
  })));
