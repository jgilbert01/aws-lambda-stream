import _ from 'highland';

import { putObjectToS3 } from './s3';
import { now } from '../utils/time';

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

const formatKey = (event) => {
  const d = new Date(now());
  // region/claimchecks/YYYY/MM/DD/HH/id
  return `${process.env.AWS_REGION}/claimchecks/${d.getUTCFullYear()}/${d.getUTCMonth()}/${d.getUTCDate()}/${d.getUTCHours()}/${event.id}`;
};

export const toClaimcheckEvent = (event, bucket) => ({
  id: event.id,
  type: event.type,
  partitionKey: event.partitionKey,
  timestamp: event.timestamp,
  tags: event.tags,
  s3: {
    bucket,
    key: formatKey(event),
  },
});

export const toPutClaimcheckRequest = (event, Bucket) => ({
  Bucket,
  Key: formatKey(event),
  Body: JSON.stringify(event),
});

// designed to run right after batchWithSize
export const storeClaimcheck = ({
  id: pipelineId,
  // usually the s3 event lake bucket
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
        // reassemble the batch
        .collect());
  // minus any faults during putObject
};
