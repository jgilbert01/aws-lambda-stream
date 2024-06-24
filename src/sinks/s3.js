import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const putObjectToS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  putRequestField = 'putRequest',
  putResponseField = 'putResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, bucketName, ...opt });

  const putObject = (uow) => {
    if (!uow[putRequestField]) return _(Promise.resolve(uow));

    const p = connector.putObject(uow[putRequestField])
      .then((putResponse) => ({ ...uow, [putResponseField]: putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(putObject)
    .parallel(parallel);
};

export const deleteObjectFromS3 = ({
  debug,
  bucketName = process.env.BUCKET_NAME,
  deleteRequestField = 'deleteRequest',
  deleteResponseField = 'deleteResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  xrayEnabled = process.env.XRAY_ENABLED === 'true',
} = {}) => {
  const connector = new Connector({ debug, bucketName, xrayEnabled });

  const deleteObject = (uow) => {
    if (!uow[deleteRequestField]) return _(Promise.resolve(uow));

    const p = connector.deleteObject(uow[deleteRequestField])
      .then((deleteResponse) => ({ ...uow, [deleteResponseField]: deleteResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(deleteObject)
    .parallel(parallel);
};
