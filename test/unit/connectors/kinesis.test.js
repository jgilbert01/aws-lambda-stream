import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import Promise from 'bluebird';

import Publisher from '../../../src/connectors/kinesis';

import { debug } from '../../../src/utils';

const AWS = require('aws-sdk-mock');

AWS.Promise = Promise;

describe('connectors/kinesis.js', () => {
  afterEach(() => {
    AWS.restore('Kinesis');
  });

  it('should publish', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('Kinesis', 'putRecords', spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
          PartitionKey: '1',
        },
      ],
    };

    const data = await new Publisher({
      debug: debug('kinesis'),
      streamName: 's1',
    }).putRecords(inputParams);

    expect(spy).to.have.been.calledWith({
      StreamName: 's1',
      Records: inputParams.Records,
    });
    expect(data).to.deep.equal({});
  });
});
