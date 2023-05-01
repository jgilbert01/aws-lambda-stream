import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from './faults';
import { debug as d } from './print';
import { ratelimit } from './ratelimit';

export const putObjectToS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  putRequestField = 'putRequest',
  putResponseField = 'putResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, bucketName });

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
} = {}) => {
  const connector = new Connector({ debug, bucketName });

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

export const toGetObjectRequest = (uow) => ({
  ...uow,
  getRequest: {
    Bucket: uow.record.s3.bucket.name,
    Key: uow.record.s3.object.key,
  },
});

export const toGetObjectRequest2 = (uow) => ({
  ...uow,
  getRequest: {
    Bucket: uow.record.s3.s3.bucket.name,
    Key: uow.record.s3.s3.object.key,
  },
});

export const getObjectFromS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  getRequestField = 'getRequest',
  getResponseField = 'getResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const getObject = (uow) => {
    if (!uow[getRequestField]) return _(Promise.resolve(uow));

    const p = connector.getObject(uow[getRequestField])
      .then((getResponse) => ({ ...uow, [getResponseField]: getResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(getObject)
    .parallel(parallel);
};

export const splitS3Object = ({
  delimiter = '\n',
  getResponseField = 'getResponse',
} = {}) => (uow) => {
  const { Body, ...rest } = uow[getResponseField];
  return _(
    Buffer.from(Body).toString()
      .split(delimiter)
      .filter((line) => line.length !== 0)
      .map((line) => ({
        ...uow,
        [getResponseField]: {
          ...rest,
          line,
        },
      })),
  );
};

export const split = splitS3Object; // deprecated export

export const listObjectsFromS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  listResponseField = 'listResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const listObjects = (uow) => {
    /* istanbul ignore if */
    if (!uow[listRequestField]) return _(Promise.resolve(uow));

    const p = connector.listObjects(uow[listRequestField])
      .then((listResponse) => ({ ...uow, [listResponseField]: listResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(listObjects)
    .parallel(parallel);
};

export const pageObjectsFromS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const listObjects = (uow) => {
    let { ContinuationToken } = uow[listRequestField];

    return _((push, next) => {
      const params = {
        ...uow[listRequestField],
        ContinuationToken,
      };

      connector.listObjects(params)
        .then((data) => {
          const { Contents, ...rest } = data;

          debug('listObjects: %j', rest);

          if (rest.IsTruncated) {
            ContinuationToken = rest.NextContinuationToken;
          } else {
            ContinuationToken = undefined;
          }

          Contents.forEach((obj) => {
            push(null, {
              ...uow,
              [listRequestField]: params,
              listResponse: {
                ...rest,
                Content: obj,
              },
            });
          });
        })
        .catch(/* istanbul ignore next */(err) => {
          err.uow = uow;
          push(err, null);
        })
        .finally(() => {
          if (ContinuationToken) {
            next();
          } else {
            push(null, _.nil);
          }
        });
    });
  };

  return (s) => s
    .map(listObjects)
    .parallel(parallel);
};
