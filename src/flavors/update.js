import _ from 'highland';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream,
  query, update as updateDynamoDB, batchGet,
} from '../utils';

import { filterOnEventType, filterOnContent } from '../filters';

import { normalize } from './correlate';

// TODO qualify dynamodb utils in v1 and rename this back to update
export const upd = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  // reacting to collected events vs change events
  .map((uow) => (uow.record.dynamodb.Keys.sk.S === 'EVENT' ? /* istanbul ignore next */ normalize(uow) : uow))

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toQuery(rule))
  .through(query(rule))

  .map(toGetRequest(rule))
  .through(batchGet(rule))

  .map(toUpdateRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(updateDynamoDB(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toQuery = (rule) => (uow) => ({
  ...uow,
  queryRequest:
    rule.toQueryRequest
      ? rule.toQueryRequest(uow, rule)
      : undefined,
});

const toGetRequest = (rule) => (uow) => ({
  ...uow,
  batchGetRequest:
    rule.toGetRequest
      ? rule.toGetRequest(uow, rule)
      : undefined,
});

const toUpdateRequest = (rule) => faultyAsyncStream((uow) => Promise.resolve(rule.toUpdateRequest(uow, rule))
  .then((updateRequest) => ({
    ...uow,
    updateRequest,
  })));
