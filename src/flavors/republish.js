import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
} from '../utils';

import {
  outSourceIsSelf,
  filterOnEventType,
  filterOnContent,
  outSkip,
} from '../filters';

/**
 * used in ESG services
 * transforms and (re)publishes events
 * used in listener functions
 *
 * interface Rule {
 *   id: string
 *   flavor: republish
 *   eventType: string | string[] | Function
 *   filters?: Function[]
 *   toEvent?: string | Function
 *   source?: string // default 'custom'
 * }
 *
 */

export const republish = (rule) => (s) => // eslint-disable-line import/prefer-default-export
  s
    .filter(outSkip)
    .filter(outSourceIsSelf)

    .filter(onEventType(rule))
    .tap(printStartPipeline)

    .filter(onContent(rule))

    .flatMap(toEvent(rule))
    .through(rule.publish({ source: rule.source }))

    .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toEvent = (rule) =>
  faultyAsyncStream(async (uow) =>
    (!rule.toEvent
      ? uow
      : {
        ...uow,
        event: {
          ...uow.event,
          ...(typeof rule.toEvent === 'string'
            ? { type: rule.toEvent }
            : await faultify(rule.toEvent)(uow, rule)),
        },
      }));
