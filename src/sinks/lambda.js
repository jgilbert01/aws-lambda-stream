import _ from 'highland';

import Connector from '../connectors/lambda';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const invokeLambda = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('lambda'),
  invokeField = 'invokeRequest',
  parallel = Number(process.env.LAMBDA_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'invoke',
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, ...opt });

  const invoke = (uow) => {
    const p = () => connector.invoke(uow[invokeField], uow)
      .then((invokeResponse) => ({ ...uow, invokeResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};

export const createEventSourceMapping = ({
  id: pipelineId,
  debug = d('lambda'),
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, ...opt });

  const invoke = (uow) => {
    if (!uow.createRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.createRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = () => connector.createEventSourceMapping(params, uow)
      .then((createResponse) => ({ ...uow, createResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};

export const updateEventSourceMapping = ({
  id: pipelineId,
  debug = d('lambda'),
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, ...opt });

  const invoke = (uow) => {
    if (!uow.updateRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.updateRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = () => connector.updateEventSourceMapping(params, uow)
      .then((updateResponse) => ({ ...uow, updateResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};

export const deleteEventSourceMapping = ({
  id: pipelineId,
  debug = d('lambda'),
  step = 'delete',
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, ...opt });

  const invoke = (uow) => {
    if (!uow.deleteRequest) return _(Promise.resolve(uow));

    const { region, ...params } = uow.deleteRequest;
    connector.client.config.region = region || process.env.AWS_REGION;

    const p = () => connector.deleteEventSourceMapping(params, uow)
      .then((deleteResponse) => ({ ...uow, deleteResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s.flatMap(invoke);
};
