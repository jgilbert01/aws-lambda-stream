import {
  printStartPipeline, printEndPipeline,
  faulty,
  putObjectToS3, getObjectFromS3, deleteObjectFromS3,
  splitObject,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip,
} from '../filters';

export const materializeS3 = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(splitObject(rule))

  .map(toGetRequest(rule))
  .through(getObjectFromS3(rule))

  .map(toPutRequest(rule))
  .through(putObjectToS3(rule))

  .map(toDeleteRequest(rule))
  .through(deleteObjectFromS3(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toGetRequest = (rule) => faulty((uow) => ({
  ...uow,
  getRequest:
    rule.toGetRequest
      ? /* istanbul ignore next */ rule.toGetRequest(uow, rule)
      : undefined,
}));

const toPutRequest = (rule) => faulty((uow) => ({
  ...uow,
  putRequest:
    rule.toPutRequest
      ? /* istanbul ignore next */ rule.toPutRequest(uow, rule)
      : undefined,
}));

const toDeleteRequest = (rule) => faulty((uow) => ({
  ...uow,
  deleteRequest:
    rule.toDeleteRequest
      ? /* istanbul ignore next */ rule.toDeleteRequest(uow, rule)
      : undefined,
}));
