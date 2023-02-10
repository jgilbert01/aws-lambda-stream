import _ from 'highland';

import Connector from '../connectors/eventbridge';

import { toBatchUow, unBatchUow, batchWithSize } from './batch';
import { rejectWithFault } from './faults';
import { debug as d } from './print';
import { adornStandardTags } from './tags';
import { compress } from './compression';

export const publishToEventBridge = ({ // eslint-disable-line import/prefer-default-export
  debug = d('eventbridge'),
  busName = process.env.BUS_NAME || 'undefined',
  source = 'custom', // could change this to internal vs external/ingress/egress
  eventField = 'event', // is often named emit
  publishRequestEntryField = 'publishRequestEntry',
  publishRequestField = 'publishRequest', // was inputParams
  maxPublishRequestSize = Number(process.env.PUBLISH_MAX_REQ_SIZE) || Number(process.env.MAX_REQ_SIZE) || 256000,
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 10,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  retryConfig,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, retryConfig });

  const toPublishRequestEntry = (uow) => ({
    ...uow,
    [publishRequestEntryField]: {
      EventBusName: busName,
      Source: source,
      DetailType: uow[eventField].type,
      Detail: JSON.stringify(uow[eventField], compress(opt)),
    },
  });

  const toPublishRequest = (batchUow) => ({
    ...batchUow,
    [publishRequestField]: {
      Entries: batchUow.batch
        .map((uow) => uow[publishRequestEntryField]),
    },
  });

  const putEvents = (batchUow) => {
    const p = connector.putEvents(batchUow[publishRequestField])
      .catch(rejectWithFault(batchUow, !handleErrors))
      .then((publishResponse) => ({ ...batchUow, publishResponse }));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .filter((uow) => uow[eventField])

    .map(adornStandardTags(eventField))

    .map(toPublishRequestEntry)
    .consume(batchWithSize({
      ...opt,
      batchSize,
      maxRequestSize: maxPublishRequestSize,
      requestEntryField: publishRequestEntryField,
      requestField: publishRequestField,
      debug,
    }))
    .map(toBatchUow)
    .map(toPublishRequest)

    .map(putEvents)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};
