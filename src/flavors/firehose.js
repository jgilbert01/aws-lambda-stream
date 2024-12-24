import {
  printStartPipeline, printEndPipeline,
  faulty,
} from '../utils';
import {
  filterOnEventType, filterOnContent,
  prefilterOnEventTypes, prefilterOnContent,
} from '../filters';

export const firehoseTransform = (rule) => (s) => s
  .filter(onEventType(rule))
  .tap(printStartPipeline)

  .filter(onContent(rule))

  .map(transform(rule))

  .tap(printEndPipeline);

export const firehoseDrop = (rules) => (opt) => (s) => s
  .filter((uow) => !(prefilterOnEventTypes(rules)(uow) && prefilterOnContent(rules)(uow)))
  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

export const spreadDateTime = (dt) => {
  const date = new Date(dt);

  return {
    year: `${date.getUTCFullYear()}`,
    month: `${date.getUTCMonth() + 1}`.padStart(2, '0'), // JavaScript months are 0-indexed
    day: `${date.getUTCDate()}`.padStart(2, '0'),
    hour: `${date.getUTCHours()}`.padStart(2, '0'),
    minute: `${date.getUTCMinutes()}`.padStart(2, '0'),
  };
};

const metadata = (rule, uow) => (rule.metadata
  ? /* istanbul ignore next */ rule.metadata(uow, rule)
  : {
    partitionKeys: {
      table: rule.tableName,
      ...spreadDateTime(uow.event.timestamp),
    },
  });

const transform = (rule) => faulty((uow) => {
  const transformed = rule.transform
    ? rule.transform(uow, rule)
    : /* istanbul ignore next */ {};

  return {
    ...uow,
    transformed,
    result: 'Ok',
    data: Buffer.from(JSON.stringify(transformed), 'utf-8').toString('base64'),
    metadata: metadata(rule, uow),
  };
});
