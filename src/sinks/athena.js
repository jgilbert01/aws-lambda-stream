import _ from 'highland';
import { isServerError, isThrottlingError, isTransientError } from '@smithy/service-error-classification';
import {
  rejectWithFault,
  ratelimit,
} from '../utils';

import Connector from '../connectors/athena';

export const startQueryExecution = ({
  id: pipelineId,
  debug,
  queryRequestField = 'queryRequest',
  queryResponseField = 'queryResponse',
  parallel = Number(process.env.ATHENA_PARALLEL) || Number(process.env.PARALLEL) || 1,
  outputLocation = 'outputLocationRequest',
  encryptionEnabled = true,
  encryptionOption = 'CSE_KMS',
  kmsKey = process.env.REGIONAL_MASTER_KEY_ARN || /* istanbul ignore next */ process.env.MASTER_KEY_ARN,
  database = process.env.GLUE_DATABASE,
  catalog = process.env.GLUE_CATALOG,
  skipRetry = process.env.SKIP_RETRY === 'true' || process.env.NODE_ENV === 'test',
  ...opt
} /* = {} */) => {
  const connector = new Connector({
    pipelineId, debug, ...opt,
  });

  const invoke = (uow) => {
    /* istanbul ignore if */
    if (!uow[queryRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.startQueryExecution({
      ClientRequestToken: `${uow.record?.dynamodb?.Keys?.sk?.S}-${pipelineId}`,
      WorkGroup: process.env.WORK_GROUP,
      ResultConfiguration: {
        OutputLocation: uow[outputLocation],
        EncryptionConfiguration: encryptionEnabled ? {
          EncryptionOption: encryptionOption,
          KmsKey: kmsKey,
        } : /* istanbul ignore next */ undefined,
      },
      QueryExecutionContext: {
        Database: database,
        Catalog: catalog,
      },

      ...uow[queryRequestField],
    }, uow)
      .then((queryResponse) => ({ ...uow, [queryResponseField]: queryResponse }))
      .catch(/* istanbul ignore next */(err) => {
        if (!skipRetry && (isThrottlingError(err) || isServerError(err) || isTransientError(err))) { // TODO potentially check retry count
          return {
            ...uow,
            [queryResponseField]: { retry: uow.record.eventID },
          };
        }
        return Promise.reject(err);
      })
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, 'query') || p()); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};
