import Promise from 'bluebird';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsync,
  update,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip, outSourceIsSelf,
} from '../filters';

const materialize = (rule) => (s) => s
  .filter(outSkip)
  .filter(outSourceIsSelf)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toUpdateRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .map(update(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toUpdateRequest = (rule) => faultyAsync((uow) =>
  Promise.resolve(rule.toUpdateRequest(uow, rule))
    .then((updateRequest) => ({
      ...uow,
      updateRequest,
    })));

export default materialize;
