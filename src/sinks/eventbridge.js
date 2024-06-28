import _ from 'highland';

import Connector from '../connectors/eventbridge';

import { toBatchUow, unBatchUow, batchWithSize } from '../utils/batch';
import { rejectWithFault, FAULT_COMPRESSION_IGNORE } from '../utils/faults';
import { debug as d } from '../utils/print';
import { adornStandardTags } from '../utils/tags';
import { compress } from '../utils/compression';
import { ratelimit } from '../utils/ratelimit';

export const publishToEventBridge = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('eventbridge'),
  busName = process.env.BUS_NAME || 'undefined',
  source = process.env.BUS_SRC || 'custom', // could change this to internal vs external/ingress/egress
  eventField = 'event', // is often named emit
  publishRequestEntryField = 'publishRequestEntry',
  publishRequestField = 'publishRequest', // was inputParams
  maxPublishRequestSize = Number(process.env.PUBLISH_MAX_REQ_SIZE) || Number(process.env.MAX_REQ_SIZE) || 256 * 1024,
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 10,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  retryConfig,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, retryConfig, ...opt,
  });

  const toPublishRequestEntry = (uow) => ({
    ...uow,
    [publishRequestEntryField]: uow[eventField] ? {
      EventBusName: busName,
      Source: source,
      DetailType: uow[eventField].type,
      Detail: JSON.stringify(uow[eventField],
        compress(uow[eventField].type !== 'fault' ? opt : { ...opt, compressionIgnore: FAULT_COMPRESSION_IGNORE })),
    } : undefined,
  });

  const toPublishRequest = (batchUow) => ({
    ...batchUow,
    [publishRequestField]: {
      Entries: batchUow.batch
        .filter((uow) => uow[publishRequestEntryField])
        .map((uow) => uow[publishRequestEntryField]),
    },
  });

  const putEvents = (batchUow) => {
    if (!batchUow[publishRequestField].Entries.length) {
      return _(Promise.resolve(batchUow));
    }
    const p = connector.putEvents(batchUow[publishRequestField])
      .catch(rejectWithFault(batchUow, !handleErrors))
      .then((publishResponse) => ({ ...batchUow, publishResponse }));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(adornStandardTags(eventField))

    .through(ratelimit(opt))

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
