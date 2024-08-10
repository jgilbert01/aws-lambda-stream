import _ from 'highland';

import Connector from '../connectors/eventbridge';

import { toBatchUow, unBatchUow, batchWithSize } from '../utils/batch';
import { rejectWithFault, FAULT_COMPRESSION_IGNORE } from '../utils/faults';
import { debug as d } from '../utils/print';
import { adornStandardTags } from '../utils/tags';
import { compress } from '../utils/compression';
import { ratelimit } from '../utils/ratelimit';
import { submitClaimcheck } from './claimcheck';

export const publishToEventBridge = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('eventbridge'),
  busName = process.env.BUS_NAME || 'undefined',
  source = process.env.BUS_SRC || 'custom', // could change this to internal vs external/ingress/egress
  eventField = 'event', // is often named emit
  claimcheckEventField = 'claimcheckEvent',
  claimcheckRequiredField = 'claimcheckRequired',
  publishRequestEntryField = 'publishRequestEntry',
  publishRequestField = 'publishRequest', // was inputParams
  maxPublishRequestSize = Number(process.env.PUBLISH_MAX_REQ_SIZE) || Number(process.env.MAX_REQ_SIZE) || 256 * 1024,
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 10,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  retryConfig,
  step = 'publish',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, retryConfig, ...opt,
  });

  const compressionCandidate = (uow) => !uow[claimcheckEventField] && uow[eventField] !== 'fault';
  const compressionOpts = (uow) => (uow[eventField].type !== 'fault' ? opt : { ...opt, compressionIgnore: FAULT_COMPRESSION_IGNORE });

  const toPublishDetail = (uow) => JSON.stringify(
    uow[claimcheckEventField] ?? uow[eventField],
    compressionCandidate(uow) ? compress(compressionOpts(uow)) : null,
  );

  // If we didn't just claimcheck, and we already have a publish request enty, this is a noop.
  const toPublishRequestEntry = (uow) => ((!uow[claimcheckRequiredField] && uow[publishRequestEntryField])
    ? uow
    : ({
      ...uow,
      [publishRequestEntryField]: uow[eventField] ? {
        EventBusName: busName,
        Source: source,
        DetailType: uow[eventField].type,
        Detail: toPublishDetail(uow),
      } : undefined,
    }));

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
    const p = () => connector.putEvents(batchUow[publishRequestField], batchUow)
      .catch(rejectWithFault(batchUow, !handleErrors))
      .then((publishResponse) => ({ ...batchUow, publishResponse }));

    return _(batchUow.batch[0].metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  const adornClaimcheckRequiredFlag = (uow) => ({
    ...uow,
    [claimcheckRequiredField]: uow[publishRequestEntryField]
      ? Buffer.byteLength(JSON.stringify(uow[publishRequestEntryField])) > maxPublishRequestSize : false,
  });

  return (s) => s
    .map(adornStandardTags(eventField))

    .through(ratelimit(opt))

    // Figure out if the publish request entry is greater than the max publish request size
    // If so, perform a claimcheck on the original event, compute a new publish request entry.
    // Can't just run through claimcheck first pass, because we're not sure what the size of the
    // rest of the publish request entry is.
    .map(toPublishRequestEntry)
    .map(adornClaimcheckRequiredFlag)
    .through(submitClaimcheck({
      id: pipelineId,
      debug,
      eventField,
      claimcheckEventField,
      claimcheckRequiredField,
      step,
      ...opt,
    }))
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
