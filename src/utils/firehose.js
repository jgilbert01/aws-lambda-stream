import _ from 'highland';

import Connector from '../connectors/firehose';

import { toBatchUow, unBatchUow } from './batch';
import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const sendToFirehose = ({
  debug = d('firehose'),
  deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
  eventField = 'event',
  batchSize = Number(process.env.FIREHOSE_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.FIREHOSE_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
} = {}) => {
  const connector = new Connector({ debug, deliveryStreamName });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Records: batchUow.batch
        .map((uow) => toFirehoseRecord(uow[eventField])),
    },
  });

  const putRecordBatch = (batchUow) => {
    const p = connector.putRecordBatch(batchUow.inputParams)
      .then((putResponse) => ({ ...batchUow, putResponse }))
      .catch(rejectWithFault(batchUow, !handleErrors));
    // .then(handleFailedPutCount);

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .batch(batchSize)
    .map(toBatchUow)

    .map(toInputParams)
    .map(putRecordBatch)
    .parallel(parallel)

    .flatMap(unBatchUow); // for cleaner logging and testing
};

// TODO
// const handleFailedPutCount = (putResponse) => {
//   if (putResponse.FailedPutCount > 0) {
//     // const e = new Error();
//     // TODO
//     // raise error to resend (will create dups) or
//     // resend here (will eventually timeout) and/or
//     // raise fault with failed events
//   }
//   return putResponse;
// };

export const toFirehoseRecord = (e) => ({
  Data: Buffer.from(`${JSON.stringify(e)}\n`),
});
