import { toGetObjectRequest, getObjectFromS3, split } from 'aws-lambda-stream';

const pipeline = options => stream => stream
  .map(toGetObjectRequest)
  .through(getObjectFromS3(options))
  .flatMap(split(options))
  .map(toEvent);
// .tap(uow => console.log(JSON.stringify(uow, null, 2)));
// .tap(uow => console.log('%j', uow));

// TODO error handling
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

// uow => ({
//   ...uow,
//   event: uow.getResponse.err ? {} : JSON.parse(Buffer.from(uow.getResponse.Body)),
// });

export default pipeline;
