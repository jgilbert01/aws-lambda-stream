import _ from 'highland';

import Connector from '../connectors/timestream';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';

export const writeRecords = ({ // eslint-disable-line import/prefer-default-export
  id: pipelineId,
  debug = d('ts'),
  writeRequestField = 'writeRequest',
  writeResponseField = 'writeResponse',
  parallel = Number(process.env.TIMESTREAM_PARALLEL) || Number(process.env.PARALLEL) || 8,
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({ pipelineId, debug, ...opt });

  const write = (uow) => {
    // istanbul ignore next
    if (!uow[writeRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.writeRecords(uow[writeRequestField])
      .then((writeResponse) => ({ ...uow, [writeResponseField]: writeResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(write)
    .parallel(parallel);
};
