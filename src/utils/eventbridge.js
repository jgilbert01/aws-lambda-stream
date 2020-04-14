import _ from 'highland';

import Connector from '../connectors/eventbridge';

import { toBatchUow, unBatchUow } from './batch';
import { rejectWithFault, throwFault } from './faults';
import { debug as d } from './print';
import { adornStandardTags } from './tags';

export const publishToEventBridge = ({ // eslint-disable-line import/prefer-default-export
  debug = d('eventbridge'),
  busName = process.env.BUS_NAME || 'undefined',
  source = 'custom',
  eventField = 'event',
  batchSize = Number(process.env.PUBLISH_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.PUBLISH_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
} = {}) => {
  const connector = new Connector({ debug });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Entries: batchUow.batch
        .map((uow) => ({
          EventBusName: busName,
          Source: source,
          DetailType: uow[eventField].type,
          Detail: JSON.stringify(uow[eventField]),
        })),
    },
  });

  const putEvents = (batchUow) => {
    const p = connector.putEvents(batchUow.inputParams)
      .catch(rejectWithFault(batchUow, !handleErrors))
      .then(handleFailedEntries(batchUow))
      .then((publishResponse) => ({ ...batchUow, publishResponse }));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(adornStandardTags(eventField))

    .batch(batchSize)
    .map(toBatchUow)

    .map(toInputParams)
    .map(putEvents)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};

const handleFailedEntries = (batchUow) => (publishResponse) => {
  if (publishResponse.FailedEntryCount === 0) {
    return publishResponse;
  } else {
    const failed = publishResponse.Entries.reduce((a, c, i) => {
      if (c.ErrorCode) {
        return [...a, {
          ...batchUow.batch[i],
          inputParam: batchUow.inputParams.Entries[i],
          err: {
            code: c.ErrorCode,
            msg: c.ErrorMessage,
          },
        }];
      } else {
        return a;
      }
    }, []);

    return throwFault({ batch: failed })(new Error(`Event Bridge Failed Entry Count: ${publishResponse.FailedEntryCount}`));
  }
};
