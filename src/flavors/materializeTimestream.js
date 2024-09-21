import {
  printStartPipeline, printEndPipeline,
  faulty,
  splitObject, compact,
} from '../utils';
import {
  writeRecords,
} from '../sinks/timestream';
import {
  filterOnEventType, filterOnContent,
} from '../filters';

export const materializeTimestream = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(compact(rule))
  .through(splitObject(rule))

  .map(toWriteRequest(rule))
  .through(writeRecords(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toWriteRequest = (rule) => faulty((uow) => ({
  ...uow,
  writeRequest: rule.toWriteRequest(uow, rule),
}));
