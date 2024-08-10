import { putObjectToS3 } from './s3';

export const submitClaimcheck = ({
  id: pipelineId,
  bucketName = process.env.CLAIMCHECK_BUCKET_NAME || /* istanbul ignore next */ process.env.BUCKET_NAME,
  putRequestField = 'putClaimcheckRequest',
  putResponseField = 'putClaimcheckResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  /* Parent sink owns the some properties, so defaults not provided. */
  debug,
  step,
  eventField,
  claimcheckEventField,
  claimcheckRequiredField,
  ...opt
}) => {
  // If we don't have a bucket...we can't claimcheck.
  if (!bucketName) return (s) => s;

  const toClaimcheckRequest = (uow) => {
    if (!uow[eventField] || !uow[claimcheckRequiredField]) return uow;

    return {
      ...uow,
      [putRequestField]: {
        Key: `CLAIMCHECK-${uow[eventField].id}`,
        Body: JSON.stringify(uow[eventField]),
      },
    };
  };

  const recordClaimcheck = (uow) => {
    if (!uow[putResponseField] || !uow[putRequestField]) return uow;

    return {
      ...uow,
      [claimcheckEventField]: {
        // We still need some of the attributes from the underlying envelope
        // to allow it to route/partition properly to downstream services.
        id: uow[eventField].id,
        type: uow[eventField].type,
        partitionKey: uow[eventField].partitionKey,
        timestamp: uow[eventField].timestamp,
        s3: {
          bucket: bucketName,
          key: uow[putRequestField].Key,
        },
      },
    };
  };

  return (s) => s
    .map(toClaimcheckRequest)
    .through(putObjectToS3({
      debug,
      id: pipelineId,
      bucketName,
      putRequestField,
      putResponseField,
      parallel,
      step,
      ...opt,
    }))
    .map(recordClaimcheck);
};
