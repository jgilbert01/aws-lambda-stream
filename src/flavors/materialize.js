import Promise from 'bluebird';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsync,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip, outSourceIsSelf,
} from '../filters';

import { update } from '../utils/dynamodb';

export const materialize = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)
  .filter(outSourceIsSelf)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toUpdateRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(update(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toUpdateRequest = (rule) => faultyAsync((uow) =>
  Promise.resolve(rule.toUpdateRequest(uow, rule))
    .then((updateRequest) => ({
      ...uow,
      updateRequest,
    })));
