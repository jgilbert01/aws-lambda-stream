import _ from 'highland';

import Connector from '../connectors/sns';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const publishToSns = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('sns'),
  topicArn = process.env.TOPIC_ARN,
  messageField = 'message',
  parallel = Number(process.env.SNS_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, topicArn });

  const publish = (uow) => {
    const p = connector.publish(uow[messageField])
      .then((publishResponse) => ({ ...uow, publishResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(publish)
    .parallel(parallel);
};
