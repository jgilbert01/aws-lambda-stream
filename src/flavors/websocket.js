import _ from 'highland';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
} from '../utils';

import { publishToConnections } from '../sinks/websocket';
import { filterOnEventType, filterOnContent } from '../filters';

export const broadcastToWebSocket = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(toMessage(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .flatMap(toConnections(rule))

  .through(publishToConnections(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toMessage = (rule) => faultyAsyncStream(async (uow) => ({
  ...uow,
  message: await faultify(rule.toMessage)(uow, rule),
}));

// rule.toConnections returns a promise resolving to an array of { connectionId }
// flatMap fans out the uow into one per connection
const toConnections = (rule) => (uow) => {
  if (!rule.toConnections) return _([uow]);
  const p = faultify(rule.toConnections)(uow, rule)
    .then((connections) => connections.map((conn) => ({
      ...uow,
      connectionId: conn.connectionId,
    })));
  return _(p).flatten();
};
