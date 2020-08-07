import 'mocha';
import { expect } from 'chai';
import { toDynamodbRecords } from 'aws-lambda-stream';

import { handle } from '../../../src/trigger';

describe('trigger/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test trigger integration', (done) => {
    handle(EVENT, {}, (err, res) => {
      expect(res).to.equal('Success');
      done();
    });
  });
});

// TODO copy from logs
const EVENT = toDynamodbRecords([
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
