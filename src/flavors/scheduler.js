import _ from 'highland';

import {
  filterOnEventType, filterOnContent,
} from '../filters';
import {
  printStartPipeline,
  printEndPipeline,
  faulty,
} from '../utils';

import { scheduleEvent } from '../sinks/scheduler';

export const scheduler = (rule) => (s) => s
  .filter(onEventType(rule))
  .filter(onContent(rule))
  .tap(printStartPipeline)

  .map(toScheduleRequest(rule))
  .through(scheduleEvent(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toScheduleRequest = (rule) => faulty((uow) => ({
  ...uow,
  scheduleRequest:
        rule.toScheduleRequest
          ? rule.toScheduleRequest(uow, rule)
          : /* istanbul ignore next */ undefined,
}));
