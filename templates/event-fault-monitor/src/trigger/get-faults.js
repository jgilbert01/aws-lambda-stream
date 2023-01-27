import { getObjectFromS3, split } from 'aws-lambda-stream';

const pipeline = (options) => (stream) => stream
  .map(toGetObjectRequest)
  .through(getObjectFromS3(options))
  .flatMap(split(options))
  .map(toEvent);

const toGetObjectRequest = (uow) => ({
  ...uow,
  getRequest: {
    Bucket: uow.record.s3.s3.bucket.name,
    Key: uow.record.s3.s3.object.key,
  },
});

const toEvent = (uow) => {
  const { detail, ...eb } = JSON.parse(uow.getResponse.line);
  return ({
    ...uow,
    record: {
      ...uow.record,
      eb,
    },
    event: detail,
  });
};

export default pipeline;
