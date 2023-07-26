import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
  updateDynamoDB, splitObject,
  scanSplitDynamoDB, querySplitDynamoDB, queryAllDynamoDB, batchGetDynamoDB,
  encryptEvent,
} from '../utils';

import { filterOnEventType, filterOnContent } from '../filters';

export const job = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  // scan for records of interest
  .map(toScanRequest(rule))
  .through(scanSplitDynamoDB(rule))

  // or query for records of interest
  .map(toQuerySplitRequest(rule))
  .through(querySplitDynamoDB(rule))

  .filter(onContent(rule))

  // query related data for the current uow
  .map(toQueryRelatedRequest(rule))
  .through(queryAllDynamoDB(rule))

  .through(splitObject({
    splitTargetField: rule.queryResponseField || 'queryResponse',
    ...rule,
  }))

  // get related data for the current uow
  .map(toGetRequest(rule))
  .through(batchGetDynamoDB(rule))

  // write back to the db
  .map(toUpdateRequest(rule))
  .through(updateDynamoDB(rule))

  // or publish an event
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

const toScanRequest = (rule) => faulty((uow) => ({
  ...uow,
  scanRequest:
    rule.toScanRequest
      ? rule.toScanRequest(uow, rule)
      : /* istanbul ignore next */ undefined,
}));

const toQuerySplitRequest = (rule) => faulty((uow) => ({
  ...uow,
  querySplitRequest:
    rule.toQuerySplitRequest
      ? rule.toQuerySplitRequest(uow, rule)
      : /* istanbul ignore next */ undefined,
}));

const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toQueryRelatedRequest = (rule) => faulty((uow) => ({
  ...uow,
  queryRequest:
    (rule.toQueryRelatedRequest || rule.toQueryRequest)
      ? /* istanbul ignore next */ (rule.toQueryRelatedRequest || rule.toQueryRequest)(uow, rule)
      : undefined,
}));

const toGetRequest = (rule) => faulty((uow) => ({
  ...uow,
  batchGetRequest:
    rule.toGetRequest
      ? /* istanbul ignore next */ rule.toGetRequest(uow, rule)
      : undefined,
}));

const toUpdateRequest = (rule) => faulty((uow) => ({
  ...uow,
  updateRequest:
    rule.toUpdateRequest
      ? /* istanbul ignore next */ rule.toUpdateRequest(uow, rule)
      : undefined,
}));

const toEvent = (rule) => faultyAsyncStream(async (uow) => (!rule.toEvent
  ? /* istanbul ignore next */ uow
  : ({
    ...uow,
    emit: {
      ...await faultify(rule.toEvent)(uow, rule),
    },
  })));
