import _ from 'highland';

import Connector from '../connectors/websocket';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const publishToConnections = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('ws'),
  endpoint = process.env.WEBSOCKET_ENDPOINT,
  messageField = 'message',
  parallel = Number(process.env.WS_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'postToConnection',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, endpoint, ...opt,
  });

  const post = (uow) => {
    const p = () => connector.postToConnection(uow.connectionId, uow[messageField], uow)
      .then((postResponse) => ({ ...uow, postResponse }))
      .catch((err) => {
        // 410 = connection is gone, clean up silently
        if (err.statusCode === 410 || err.$metadata?.httpStatusCode === 410) {
          return { ...uow, postResponse: { statusCode: 410, connectionId: uow.connectionId } };
        }
        return rejectWithFault(uow)(err);
      });

    return _(uow.metrics?.w(p, step) || /* istanbul ignore next */ p()); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(post)
    .parallel(parallel);
};
