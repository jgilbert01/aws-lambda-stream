import _ from 'highland';
import memoryCache from 'memory-cache';
import {
  printStart,
  printEnd,
  publishToSns,
} from 'aws-lambda-stream';

const pipeline = (options) => (s) => s
  .tap(printStart)

  .map(toMessage)
  .filter(oncePerHalfHour)
  .through(publishToSns(options))

  .tap(printEnd);

const toMessage = (uow) => ({
  ...uow,
  message: {
    Subject: `Fault: ${uow.event.tags.account},${uow.event.tags.region},${uow.event.tags.stage},${uow.event.tags.functionname},${uow.event.tags.pipeline}`.substring(0, 100),
    Message: JSON.stringify(uow.event, null, 2),
  },
});

const oncePerHalfHour = (uow) => {
  const key = `${uow.message.Subject}`; // |${uow.event.err.message}`;
  const found = memoryCache.get(key);
  if (!found) {
    memoryCache.put(key, true, process.env.NODE_ENV === 'test'
      ? 1000
      : /* istanbul ignore next */ 1000 * 60 * 30); // 30 minutes
  }
  return !found;
};

export default pipeline;
