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
