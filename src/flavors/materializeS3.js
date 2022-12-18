import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
  putObjectToS3, splitObject,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip, outSourceIsSelf,
} from '../filters';


export const materializeS3 = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)
  // .filter(outSourceIsSelf)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(splitObject(rule))

  .map(toPutRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(putObjectToS3(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toPutRequest = (rule) => faultyAsyncStream(async (uow) => ({
  ...uow,
  putRequest: await faultify(rule.toPutRequest)(uow, rule),
}));
