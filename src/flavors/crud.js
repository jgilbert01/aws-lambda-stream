import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsync,
} from '../utils';

import { publish } from '../utils/kinesis';
import { filterOnEventType, filterOnContent, outLatched } from '../filters';

const crud = (rule) => (s) => s
  .filter(outLatched)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toEvent(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(publish(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toEvent = (rule) => faultyAsync((uow) =>
  (!rule.toEvent ? Promise.resolve(uow)
    : Promise.resolve(rule.toEvent(uow, rule))
      .then((event) => ({
        ...uow,
        event: {
          ...uow.event,
          ...event,
        },
      }))));

export default crud;
