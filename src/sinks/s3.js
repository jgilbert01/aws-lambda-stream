import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const putObjectToS3 = ({
  debug = d('s3'),
  id: pipelineId,
  bucketName = process.env.BUCKET_NAME,
  putRequestField = 'putRequest',
  putResponseField = 'putResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

  const putObject = (uow) => {
    if (!uow[putRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.putObject(uow[putRequestField], uow)
      .then((putResponse) => ({ ...uow, [putResponseField]: putResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(putObject)
    .parallel(parallel);
};

export const deleteObjectFromS3 = ({
  debug,
  id: pipelineId,
  bucketName = process.env.BUCKET_NAME,
  deleteRequestField = 'deleteRequest',
  deleteResponseField = 'deleteResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'delete',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

  const deleteObject = (uow) => {
    if (!uow[deleteRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.deleteObject(uow[deleteRequestField])
      .then((deleteResponse) => ({ ...uow, [deleteResponseField]: deleteResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(deleteObject)
    .parallel(parallel);
};

export const copyS3Object = ({
  debug,
  id: pipelineId,
  bucketName = process.env.BUCKET_NAME,
  copyRequestField = 'copyRequest',
  copyResponseField = 'copyResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'copy',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

  const copyObject = (uow) => {
    if (!uow[copyRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.copyObject(uow[copyRequestField])
      .then((copyResponse) => ({ ...uow, [copyResponseField]: copyResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p());
  };

  return (s) => s
    .map(copyObject)
    .parallel(parallel);
};
