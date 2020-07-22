import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import Promise from 'bluebird';

import Connector from '../../../src/connectors/sns';

import { debug } from '../../../src/utils';

const AWS = require('aws-sdk-mock');

AWS.Promise = Promise;

describe('connectors/sns.js', () => {
  afterEach(() => {
    AWS.restore('SNS');
  });

  it('should send msg', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('SNS', 'publish', spy);

    const inputParams = {
      Message: JSON.stringify({ f1: 'v1' }),
    };

    const data = await new Connector({
      debug: debug('sns'),
      topicArn: 't1',
    }).publish(inputParams);

    expect(spy).to.have.been.calledWith({
      TopicArn: 't1',
      Message: inputParams.Message,
    });
    expect(data).to.deep.equal({});
  });
});
