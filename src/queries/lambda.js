import _ from 'highland';

import Connector from '../connectors/lambda';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';

export const listEventSourceMappings = ({ // eslint-disable-line import/prefer-default-export
  debug = d('lambda'),
} = {}) => {
  const connector = new Connector({ debug });

  const list = (uow) => {
    const { region, ...params } = uow.listRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = connector.listEventSourceMappings(params)
      .then((listResponse) => ({ ...uow, listResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s.flatMap(list);
};
