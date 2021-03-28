import _ from 'highland';
import {
  printStart,
  printEnd,
  publishToSns,
} from 'aws-lambda-stream';

const pipeline = options => s => s
  .tap(printStart)

  .map(toMessage)
  .through(publishToSns(options))

  .tap(printEnd);

const toMessage = uow => ({
  ...uow,
  message: {
    Subject: `Fault: ${Object.values(uow.event.tags).join()}`,
    Message: uow.event,
  },
});

export default pipeline;
