import _ from 'highland';

import Connector from '../connectors/lambda';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const invokeLambda = ({
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

export const createEventSourceMapping = ({
  debug = d('lambda'),
} = {}) => {
  const connector = new Connector({ debug });

  const invoke = (uow) => {
    if (!uow.createRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.createRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = connector.createEventSourceMapping(params)
      .then((createResponse) => ({ ...uow, createResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};

export const updateEventSourceMapping = ({
  debug = d('lambda'),
} = {}) => {
  const connector = new Connector({ debug });

  const invoke = (uow) => {
    if (!uow.updateRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.updateRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = connector.updateEventSourceMapping(params)
      .then((updateResponse) => ({ ...uow, updateResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};

export const deleteEventSourceMapping = ({
  debug = d('lambda'),
} = {}) => {
  const connector = new Connector({ debug });

  const invoke = (uow) => {
    if (!uow.deleteRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.deleteRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = connector.deleteEventSourceMapping(params)
      .then((deleteResponse) => ({ ...uow, deleteResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};
