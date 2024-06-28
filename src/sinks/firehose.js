import _ from 'highland';

import Connector from '../connectors/firehose';

import { toBatchUow, unBatchUow } from '../utils/batch';
import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { compress } from '../utils/compression';

export const sendToFirehose = ({
  id: pipelineId,
  debug = d('firehose'),
  deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
  eventField = 'event',
  batchSize = Number(process.env.FIREHOSE_BATCH_SIZE) || Number(process.env.BATCH_SIZE) || 25,
  parallel = Number(process.env.FIREHOSE_PARALLEL) || Number(process.env.PARALLEL) || 8,
  handleErrors = true,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, deliveryStreamName, ...opt,
  });

  const toInputParams = (batchUow) => ({
    ...batchUow,
    inputParams: {
      Records: batchUow.batch
        .map((uow) => toFirehoseRecord(uow[eventField], opt)),
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

export const toFirehoseRecord = (e, opt) => ({
  Data: Buffer.from(`${JSON.stringify(e, compress(opt))}\n`),
});
