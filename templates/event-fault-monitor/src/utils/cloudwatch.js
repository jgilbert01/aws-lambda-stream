import _ from 'highland';
import {
  rejectWithFault,
  debug as d,
} from 'aws-lambda-stream';

import Connector from '../connectors/cloudwatch';

export const putMetric = ({ // eslint-disable-line import/prefer-default-export
  debug = d('cw'),
  parallel = Number(process.env.CW_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug });

  const put = (uow) => {
    const p = connector.put(uow.putRequest)
      .then(putResponse => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return s => s
    .map(put)
    .parallel(parallel);
};
