import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsync,
  toBatchUow, unBatchUow, publishEvents,
} from '../utils';

import { filterOnEventType, filterOnContent } from '../filters';

export const crud = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toEvent(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .batch(rule.batchSize || Number(process.env.PUBLISH_BATCH_SIZE) || 25)
  .map(toBatchUow)
  .map(publish(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .flatMap(unBatchUow)
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

const publish = (rule) =>
  publishEvents(null, rule.streamName || process.env.STREAM_NAME);
