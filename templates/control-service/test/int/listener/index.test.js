import 'mocha';
import { expect } from 'chai';
import { toKinesisRecords } from 'aws-lambda-stream';

import { handle } from '../../../src/listener';

describe('listener/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test listener integration', (done) => {
    handle(EVENT, {}, (err, res) => {
      expect(res).to.equal('Success');
      done();
    });
  });
});

// TODO copy from logs
const EVENT = toKinesisRecords([
  {
    id: '1',
    type: 'thing-submitted',
    timestamp: 1548967022000,
    partitionKey: '11',
    thing: {
      id: '11',
      name: 'Thing One',
      description: 'This is thing one',
    },
  },
]);
