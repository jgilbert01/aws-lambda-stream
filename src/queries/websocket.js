import _ from 'highland';

import Connector from '../connectors/apigatewayclient';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const queryConnection = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('ws'),
  endpoint = process.env.WEBSOCKET_ENDPOINT,
  getConnectionResponseField = 'getConnectionResponse',
  parallel = Number(process.env.WEBSOCKET_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'getConnection',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, endpoint, ...opt,
  });

  const get = (uow) => {
    if (!uow.connectionId) return _(Promise.resolve(uow));

    const p = () => connector.getConnection(uow.connectionId, uow)
      .then((getConnectionResponse) => ({ ...uow, [getConnectionResponseField]: getConnectionResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || /* istanbul ignore next */ p()); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(get)
    .parallel(parallel);
};
