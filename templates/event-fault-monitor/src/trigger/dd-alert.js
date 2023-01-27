import _ from 'highland';
import memoryCache from 'memory-cache';
import moment from 'moment';
import {
  printStart,
  printEnd,
  rejectWithFault,
} from 'aws-lambda-stream';

import Connector from '../connectors/datadog';

const pipeline = (options) => (s) => s
  .tap(printStart)

  .map(toParams)
  .filter(oncePerHalfHour)
  .through(sendToDatadog(options))

  .tap(printEnd);

const toParams = (uow) => ({
  ...uow,
  params: {
    key: [
      uow.event.tags.account,
      uow.event.tags.region,
      uow.event.tags.stage,
      uow.event.tags.functionname,
      uow.event.tags.pipeline,
    ].join(),
    title: `Fault Event: ${uow.event.err && uow.event.err.message ? uow.event.err.message : /* istanbul ignore next */ undefined}`,
    properties: {
      alert_type: 'error',
      priority: 'all',
      date_happened: moment().diff(moment(uow.event.timestamp), 'hours') > 1
        ? moment().unix()
        : /* istanbul ignore next */ moment(uow.event.timestamp).unix(),
      source_type_name: 'my apps',
      tags: [
        'event_type:fault',
        `account:${process.env.ACCOUNT_NAME}`,
        `service:${uow.event.tags.source}`,
        `env:${uow.event.tags.stage}`,
        `functionname:${uow.event.tags.functionname}`,
        `pipeline:${uow.event.tags.pipeline}`,
        `error_type:${uow.event.err ? uow.event.err.name : /* istanbul ignore next */ undefined}`,
      ],
    },
    message: uow.event.err ? uow.event.err.stack : /* istanbul ignore next */ undefined,
  },
});

const oncePerHalfHour = (uow) => {
  const key = `${uow.params.key},${uow.event.err.message}`;
  const found = memoryCache.get(key);
  if (!found) {
    memoryCache.put(key, true, process.env.NODE_ENV === 'test'
      ? 1000
      : /* istanbul ignore next */ 1000 * 60 * 30); // 30 minutes
  }
  return !found;
};

const sendToDatadog = ({
  debug,
  apiKey = process.env.DATADOG_API_KEY,
  appKey = process.env.DATADOG_APP_KEY,
}) => {
  const connector = new Connector({ debug, apiKey, appKey });

  const sendEvent = (uow) => {
    const p = connector.sendEvent(uow.params.title, uow.params.message, uow.params.properties)
      .then((datadogResponse) => ({ ...uow, datadogResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(sendEvent)
    .parallel(1);
};

export default pipeline;
