import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
  splitObject, encryptEvent,
  compact,
} from '../utils';
import {
  scanSplitDynamoDB, querySplitDynamoDB, queryAllDynamoDB, batchGetDynamoDB,
} from '../queries/dynamodb';
import {
  updateDynamoDB,
} from '../sinks/dynamodb';
import { filterOnEventType, filterOnContent } from '../filters';

export const job = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  // job level filter
  .filter(onContent({
    ...rule,
    filters: rule.jobFilters,
  }))

  // scan for records of interest
  .map(toScanRequest(rule))
  .through(scanSplitDynamoDB(rule))

  // or query for records of interest
  .map(toQuerySplitRequest(rule))
  .through(querySplitDynamoDB(rule))

  // current uow level filter
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

  .through(flushCursor(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));

const toScanRequest = (rule) => faulty((uow) => ({
  ...uow,
  scanRequest:
    rule.toScanRequest
      ? /* istanbul ignore next */ rule.toScanRequest(uow, rule)
      : undefined,
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
    /* istanbul ignore next */
    (rule.toQueryRelatedRequest || rule.toQueryRequest)
      ? (rule.toQueryRelatedRequest || rule.toQueryRequest)(uow, rule)
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

export const toCursorUpdateRequest = (rule) => faulty((uow) => ({
  ...uow,
  cursorUpdateRequest:
    rule.toCursorUpdateRequest
      ? rule.toCursorUpdateRequest(uow, rule)
      : /* istanbul ignore next */ undefined,
}));

export const flushCursor = (rule) => (s) => {
  /* istanbul ignore else */
  if (rule.toCursorUpdateRequest) {
    return s
      // Compact explicitly on PK here since we want to capture just the last event per PK in this
      // invocation after the query split. This handles the case where multiple cursor events
      // ended up in a single lambda invocation.
      .through(compact({ ...rule, compact: true }))
      .map(toCursorUpdateRequest(rule))
      .through(updateDynamoDB({
        ...rule,
        updateRequestField: 'cursorUpdateRequest',
        updateResponseField: 'cursorUpdateResponse',
      }))
      // Maintains backwards compatibility with how this used to manipulate the UOWs,
      // duping the last uow.
      .flatMap((uow) => {
        const { batch, ...lastUow } = uow;
        return [
          ...batch,
          lastUow,
        ];
      });
  } else {
    return s;
  }
};
