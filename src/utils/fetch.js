import _ from 'highland';

import Connector from '../connectors/fetch';

import { rejectWithFault } from './faults';
import { debug as d } from './print';
import { ratelimit } from './ratelimit';

export const fetch = ({ // eslint-disable-line import/prefer-default-export
  debug = d('fetch'),
  prefix = 'fetch',
  httpsAgent,
  parallel = Number(process.env.FETCH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, httpsAgent });

  const invoke = (uow) => {
    const { url, responseType = 'json', ...request } = uow[`${prefix}Request`];
    const p = connector.fetch(url, request, responseType)
      .then((response) => ({ ...uow, [`${prefix}Response`]: response }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};
