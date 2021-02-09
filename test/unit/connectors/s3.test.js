import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/s3';

import { debug } from '../../../src/utils';

describe('connectors/s3.js', () => {
  afterEach(() => {
    AWS.restore('S3');
  });

  it('should put object', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('S3', 'putObject', spy);

    const inputParams = {
      Body: JSON.stringify({ f1: 'v1' }),
      Key: 'k1',
    };

    const data = await new Connector({
      debug: debug('sqs'),
      bucketName: 'b1',
    }).putObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Body: inputParams.Body,
      Key: 'k1',
    });
    expect(data).to.deep.equal({});
  });

  it('should get object', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { Body: 'b' }));
    AWS.mock('S3', 'getObject', spy);

    const inputParams = {
      Key: 'k1',
    };

    const data = await new Connector({
      debug: debug('sqs'),
      bucketName: 'b1',
    }).getObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Key: 'k1',
    });
    expect(data).to.deep.equal({ Body: 'b' });
  });
});
