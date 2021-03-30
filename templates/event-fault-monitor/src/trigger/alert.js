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
    Subject: `Fault: ${uow.event.tags.account},${uow.event.tags.region},${uow.event.tags.stage},${uow.event.tags.functionname},${uow.event.tags.pipeline}`,
    Message: JSON.stringify(uow.event, null, 2),
  },
});

export default pipeline;
