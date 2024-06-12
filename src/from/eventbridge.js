import _ from 'highland';

import { faulty } from '../utils';

export const fromEventBridge = (event) =>

  _([{
    // create a unit-of-work for the single event
    // create a stream to work with the rest of the framework
    record: event,
  }])
    .map(faulty((uow) => ({
      ...uow,
      event: {
        id: uow.record.detail.id || uow.record.id,
        ...uow.record.detail,
      },
    })));

// test helper
export const toEventBridgeRecord = (event) => ({
  'version': '0',
  'id': '0',
  'source': 'test',
  // 'account': '0123456789012',
  // 'time': '2020-03-30T08:26:41Z',
  'region': process.env.AWS_REGION || 'us-west-2',
  // 'resources': [],
  'detail-type': event.type,
  'detail': event,
});
