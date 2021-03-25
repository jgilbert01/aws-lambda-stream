import 'mocha';
import { expect } from 'chai';

import { toS3Records } from 'aws-lambda-stream';

import { handle } from '../../../src/trigger';

describe('trigger/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test trigger integration', async () => {
    console.log(JSON.stringify(EVENT, null, 2));
    const res = await handle(EVENT, {});
    expect(res).to.equal('Success');
  });
});

const EVENT = toS3Records([
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

