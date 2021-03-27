import _ from 'highland';

import Connector from '../connectors/cloudwatch';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const putMetrics = ({ // eslint-disable-line import/prefer-default-export
  debug = d('cw'),
  putField = 'putRequest',
  parallel = Number(process.env.CW_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug });

  const put = (uow) => {
    const p = connector.put(uow[putField])
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(put)
    .parallel(parallel);
};
