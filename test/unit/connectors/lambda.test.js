import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { mockClient } from 'aws-sdk-client-mock';
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';

import Connector from '../../../src/connectors/lambda';

describe('connectors/lambda.js', () => {
  let mockLambda;

  beforeEach(() => {
    mockLambda = mockClient(LambdaClient);
  });

  afterEach(() => {
    mockLambda.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should invoke', async () => {
    const spy = sinon.spy((_) => ({
      StatusCode: 200,
      ExecutedVersion: '$LATEST',
      Payload: '"hello fred"',
    }));
    mockLambda.on(InvokeCommand).callsFake(spy);

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
