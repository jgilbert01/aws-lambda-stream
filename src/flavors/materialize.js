import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
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

const toUpdateRequest = (rule) => faultyAsyncStream(async (uow) => ({
  ...uow,
  updateRequest: await faultify(rule.toUpdateRequest)(uow, rule),
}));
