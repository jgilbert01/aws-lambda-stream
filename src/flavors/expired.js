import isString from 'lodash/isString';

import {
  printStartPipeline, printEndPipeline,
  faulty,
} from '../utils';

/**
 * 'expired' events are used to trigger further time-based processing
 * event must be flagged with expire=true or expire=some-specified-event-type
 * this is a reusable pipeline as opposed to a flavor per se
 * collect and correlate flavor rules flag events for expiration
 * leverages the DynamoDB TTL feature
 * the SLA of the TTL feature is low, so this is appropriate for event-time based logic,
 *    as opposed to processing-time based logic
 */

export const expired = (opt) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter(forExpiration)
  .tap(printStartPipeline)

  .map(toExpiredEvent)
  .through(opt.publish({ ...opt, eventField: 'emit' }))

  .tap(printEndPipeline);

const forExpiration = (uow) => {
  if (uow.record.eventName !== 'REMOVE') return false;

  const { ttl, expire } = uow.event.raw.old;

  if (ttl === undefined) {
    return false;
  }

  if (expire === undefined) {
    return false;
  }

  const removed = uow.record.dynamodb.ApproximateCreationDateTime;
  if (removed < ttl) {
    return false;
  }

  return true;
};

const toExpiredEvent = faulty((uow) => {
  const { ttl, expire, event } = uow.event.raw.old;
  const {
    id, type, timestamp,
  } = event;

  return ({
    ...uow,
    emit: {
      ...event,
      id: uow.event.id,
      type: calcType(type, expire),
      timestamp: (ttl * 1000) + (timestamp % 1000),
      triggers: [
        {
          id,
          type,
          timestamp,
        },
      ],
    },
  });
});

const calcType = (type, expire) => {
  if (isString(expire)) {
    return expire; // custom event type
  } else if (type.indexOf('.') > -1) { // conform to delimiter std
    return `${type}.expired`;
  } else {
    return `${type}-expired`;
  }
};
