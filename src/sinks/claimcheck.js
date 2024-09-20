import _ from 'highland';

import { putObjectToS3 } from './s3';

// // claim-check pattern support
// // https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

export const toClaimcheckEvent = (event, bucket) => ({
  id: event.id,
  type: event.type,
  partitionKey: event.partitionKey,
  timestamp: event.timestamp,
  tags: event.tags,
  s3: {
    bucket,
    key: `claimchecks/${event.id}`,
  },
});

export const toPutClaimcheckRequest = (event, Bucket) => ({
  Bucket,
  Key: `claimchecks/${event.id}`,
  Body: JSON.stringify(event),
});

export const storeClaimcheck = ({
  id: pipelineId,
  claimCheckBucketName = process.env.CLAIMCHECK_BUCKET_NAME,
  putClaimcheckRequest = 'putClaimcheckRequest',
  putClaimcheckResponse = 'putClaimcheckResponse',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
  debug,
  step,
  ...opt
}) => {
  // if we don't have a bucket we can't claimcheck
  if (!claimCheckBucketName) return (s) => s;

  return (s) => s
    .flatMap((batch) =>
      _(batch)
        .through(putObjectToS3({
          debug,
          id: pipelineId,
          bucketName: claimCheckBucketName,
          putRequestField: putClaimcheckRequest,
          putResponseField: putClaimcheckResponse,
          parallel,
          step,
          ...opt,
        }))
        .collect());
};
