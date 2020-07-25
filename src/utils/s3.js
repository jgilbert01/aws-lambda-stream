import _ from 'highland';

import Connector from '../connectors/s3';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const putObjectToS3 = ({ // eslint-disable-line import/prefer-default-export
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

// TODO getObjectFromS3
