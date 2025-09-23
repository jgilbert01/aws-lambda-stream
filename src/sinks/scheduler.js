import _ from 'highland';

import Connector from '../connectors/scheduler';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const scheduleEvent = ({
  id: pipelineId,
  debug = d('scheduler'),
  timeout = Number(process.env.SCHEDULER_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  parallel = Number(process.env.PARALLEL) || 4,
  scheduleRequestField = 'scheduleRequest',
  scheduleResponseField = 'scheduleResponse',
  step = 'schedule',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, ...opt,
  });

  const invoke = (uow) => {
    /* istanbul ignore next */
    if (!uow[scheduleRequestField]) return _(Promise.resolve(uow));

    const schedulePromise = connector
      .schedule(uow[scheduleRequestField])
      .then((scheduledResponse) => ({
        ...uow,
        [scheduleResponseField]: scheduledResponse,
      }))
      .catch(rejectWithFault(uow));

    return _(schedulePromise);
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};
