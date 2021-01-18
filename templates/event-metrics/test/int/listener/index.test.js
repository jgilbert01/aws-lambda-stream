import 'mocha';
import { expect } from 'chai';

import { toKinesisRecords } from 'aws-lambda-stream';

import { handle } from '../../../src/listener';

describe('listener/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test listener integration', async () => {
    console.log(JSON.stringify(EVENT, null, 2));
    const res = await handle(EVENT, {});
    expect(res).to.equal('Success');
  });
});

const EVENT = toKinesisRecords([
  {
    type: 'thing-created',
    timestamp: 1610992656000,
    tags: {
      account: 'dev',
      region: 'us-east-1',
      stg: 'stg',
      source: 's1',
      functionname: 'f1',
    },
  },
  {
    type: 'thing-updated',
    timestamp: 1610992657000,
    tags: {
      account: 'dev',
      region: 'us-east-1',
      stg: 'stg',
      source: 's1',
      functionname: 'f2',
    },
  },
]);

