import _ from 'highland';

import Connector from '../connectors/lambda';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const invokeLambda = ({ // eslint-disable-line import/prefer-default-export
  debug = d('lambda'),
  invokeField = 'invokeRequest',
  parallel = Number(process.env.LAMBDA_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ debug });

  const invoke = (uow) => {
    const p = connector.invoke(uow[invokeField])
      .then((invokeResponse) => ({ ...uow, invokeResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};
