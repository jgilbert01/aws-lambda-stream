import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const putObjectToS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  putRequestField = 'putRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const putObject = (uow) => {
    const p = connector.putObject(uow[putRequestField])
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(putObject)
    .parallel(parallel);
};

export const getObjectFromS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  getRequestField = 'getRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const getObject = (uow) => {
    const p = connector.getObject(uow[getRequestField])
      .then((getResponse) => ({ ...uow, getResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(getObject)
    .parallel(parallel);
};

export const listObjectsFromS3 = ({
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new Connector({ debug, bucketName });

  const listObjects = (uow) => {
    const p = connector.listObjects(uow[listRequestField])
      .then((listResponse) => ({ ...uow, listResponse }))
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
    let Marker;

    return _((push, next) => {
      const params = {
        ...uow[listRequestField],
        Marker,
      };

      connector.listObjects(params)
        .then((data) => {
          const { Contents, ...rest } = data;

          debug('listObjects: %j', rest);

          if (rest.IsTruncated) {
            Marker = rest.NextMarker;
          } else {
            Marker = undefined;
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
        .catch((err) => /* istanbul ignore next */ {
          err.uow = uow;
          push(err, null);
        })
        .finally(() => {
          if (Marker) {
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
