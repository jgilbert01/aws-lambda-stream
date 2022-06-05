import _ from 'highland';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream,
  query, update as updateDynamoDB, batchGet,
} from '../utils';

import { filterOnEventType, filterOnContent } from '../filters';

export const update = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
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
