import { decompress } from './compression';
import { faulty } from './faults';
import { getObjectFromS3 } from '../queries/s3';

// claim-check pattern support
// https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

export const claimcheck = (opt = {}) => (s) => s // eslint-disable-line import/prefer-default-export
  .map(faulty((uow) => ({
    ...uow,
    getClaimCheckRequest: uow.event.s3 ? {
      Bucket: uow.event.s3.bucket,
      Key: uow.event.s3.key,
    } : undefined,
  })))
  .through(getObjectFromS3({
    id: 'handler:claimcheck',
    getRequestField: 'getClaimCheckRequest',
    getResponseField: 'getClaimCheckResponse',
    additionalClientOpts: {
      followRegionRedirects: true,
    },
  }))
  .map(faulty((uow) => clear({
    ...uow,
    event: uow.getClaimCheckResponse
      ? JSON.parse(Buffer.from(uow.getClaimCheckResponse.Body), decompress)
      : uow.event,
  })));

const clear = (uow) => {
  if (uow.getClaimCheckRequest === undefined) {
    // backwards compatibility for test cases
    delete uow.getClaimCheckRequest;
    delete uow.getClaimCheckResponse;
  }
  return uow;
};
