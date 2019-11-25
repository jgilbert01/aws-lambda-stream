import _ from 'highland';

import { faulty } from '../utils';

export const fromKinesis = (event) => // eslint-disable-line import/prefer-default-export

  _(event.Records)

    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: Buffer.from(record.kinesis.data, 'base64').toString('utf8'),
      }))

    .map(faulty((uow) => ({
      ...uow,
      event: {
        id: uow.record.eventID,
        ...JSON.parse(uow.event),
      },
    })));
