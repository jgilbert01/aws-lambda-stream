import isFunction from 'lodash/isFunction';
import get from 'lodash/get';

import {
  printStartPipeline, printEndPipeline,
  faulty, faultyAsyncStream, faultify,
} from '../utils';

import {
  filterOnEventType, filterOnContent,
  outSkip, outSourceIsSelf,
} from '../filters';

import { updateDynamoDB } from '../utils/dynamodb';

export const materialize = (rule) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(outSkip)
  .filter(outSourceIsSelf)

  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .through(split(rule))

  .map(toUpdateRequest(rule))
  .parallel(rule.parallel || Number(process.env.PARALLEL) || 4)

  .through(updateDynamoDB(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

const toUpdateRequest = (rule) => faultyAsyncStream(async (uow) => ({
  ...uow,
  updateRequest: await faultify(rule.toUpdateRequest)(uow, rule),
}));

const split = ({
  splitOn,
  splitTargetField = 'split',
  ...rule
}) => {
  if (splitOn) {
    if (isFunction(splitOn)) {
      return (s) => s
        .flatMap(faulty((uow) => splitOn(uow, rule)));
    } else {
      return (s) => s
        .flatMap(faulty((uow) => {
          const values = get(uow.event, splitOn);
          return values.map((v) => ({
            ...uow,
            [splitTargetField]: v,
          }));
        }));
    }
  } else {
    return (s) => s;
  }
};
