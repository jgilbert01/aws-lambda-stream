import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import AWS from 'aws-sdk-mock';
import Promise from 'bluebird';

import Connector from '../../../src/connectors/lambda';

AWS.Promise = Promise;

describe('connectors/lambda.js', () => {
  afterEach(() => {
    AWS.restore('Lambda');
  });

  it('should put', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {
      StatusCode: 200,
      ExecutedVersion: '$LATEST',
      Payload: '"hello fred"',
    }));
    AWS.mock('Lambda', 'invoke', spy);

    const params = {
      FunctionName: 'helloworld',
      Payload: Buffer.from(JSON.stringify({ name: 'fred' })),
    };

    const data = await new Connector({ debug: debug('lambda') })
      .invoke(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      FunctionName: 'helloworld',
      Payload: Buffer.from(JSON.stringify({ name: 'fred' })),
    });
    expect(data).to.deep.equal({
      StatusCode: 200,
      ExecutedVersion: '$LATEST',
      Payload: '"hello fred"',
    });
  });
});
