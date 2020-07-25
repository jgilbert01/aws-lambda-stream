import _ from 'highland';

import Connector from '../connectors/sns';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const publishToSns = ({ // eslint-disable-line import/prefer-default-export
  debug = d('sns'),
  topicName = process.env.TOPIC_NAME,
  messageField = 'message',
  parallel = Number(process.env.SNS_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, topicName });

  const publish = (uow) => {
    const p = connector.publish(uow[messageField])
      .then((publishResponse) => ({ ...uow, publishResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(publish)
    .parallel(parallel);
};
