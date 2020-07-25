import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import Promise from 'bluebird';

import Connector from '../../../src/connectors/sqs';

import { debug } from '../../../src/utils';

const AWS = require('aws-sdk-mock');

AWS.Promise = Promise;

describe('connectors/sqs.js', () => {
  afterEach(() => {
    AWS.restore('SQS');
  });

  it('should send msg', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('SQS', 'sendMessageBatch', spy);

    const inputParams = {
      Entries: [
        {
          Id: '1',
          MessageBody: JSON.stringify({ f1: 'v1' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      queueUrl: 'q1',
    }).sendMessageBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      QueueUrl: 'q1',
      Entries: inputParams.Entries,
    });
    expect(data).to.deep.equal({});
  });
});
