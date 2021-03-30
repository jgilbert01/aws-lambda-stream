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
    bucket: {
      name: 'b1',
    },
    object: {
      key: 'k1',
    },
  },
]);

