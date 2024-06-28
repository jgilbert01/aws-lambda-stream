import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';

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
  id: pipelineId,
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  getRequestField = 'getRequest',
  getResponseField = 'getResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

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

export const getObjectFromS3AsStream = ({
  id: pipelineId,
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  getRequestField = 'getRequest',
  getResponseField = 'getResponse',
  delimiter = '\n',
  splitFilter = () => true,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

  const getObject = (uow) => {
    if (!uow[getRequestField]) return _(Promise.resolve(uow));

    const p = connector.getObjectStream(uow[getRequestField]);

    return _(p) // wrap promise in a stream
      .flatMap((readable) => _(readable)) // wrap stream in a stream
      .splitBy(delimiter)
      .filter(splitFilter)
      .map((getResponse) => ({ ...uow, [getResponseField]: getResponse }));
  };

  return (s) => s
    .flatMap(getObject);
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

export const listObjectsFromS3 = ({
  id: pipelineId,
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  listResponseField = 'listResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

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
  id: pipelineId,
  debug = d('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, bucketName, ...opt,
  });

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
