import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
  splitObject, compact,
} from '../utils';
import {
  updateDynamoDB,
} from '../sinks/dynamodb';
import {
  filterOnEventType, filterOnContent,
  outSourceIsSelf,
} from '../filters';

export const materialize = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSourceIsSelf)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(compact(rule))

  .through(splitObject(rule))

  .map(toUpdateRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(updateDynamoDB(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toUpdateRequest = (rule) => faultyAsyncStream(async (uow) => ({
  ...uow,
  updateRequest: await faultify(rule.toUpdateRequest)(uow, rule),
}));
