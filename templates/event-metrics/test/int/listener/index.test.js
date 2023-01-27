import 'mocha';
import { expect } from 'chai';

import { toKinesisRecords, toSqsEventRecords } from 'aws-lambda-stream';

import { handle } from '../../../src/listener';

describe('listener/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test listener integration', async () => {
    const res = await handle(EVENT, {}, { AES: false });
    expect(res).to.equal('Success');
  });
});

const EVENT = toSqsEventRecords([{
  id: 'a24f9cdaec8ead2781353ef13e942f42',
  type: 'thing-created',
  partitionKey: '00000000-0000-0000-0000-000000000000',
  timestamp: 1600485986000,
  tags: {
    account: 'dev',
    region: 'us-west-2',
    stage: 'stg',
    source: 'your-apps-event-metrics',
    functionname: 'undefined',
    pipeline: 't1',
    skip: false,
  },
  thing: {
    lastModifiedBy: 'offlineContext_authorizer_principalId',
    timestamp: 1600349040394,
    id: '00000000-0000-0000-0000-000000000000',
  },
}]);
