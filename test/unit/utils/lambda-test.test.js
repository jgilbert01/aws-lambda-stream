import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';

import lambdaTest from '../../../src/utils/lambda-test';

describe('utils/lambda-test.js', () => {
  let mockLambda;

  beforeEach(() => {
    mockLambda = mockClient(LambdaClient);
  });

  afterEach(() => {
    mockLambda.restore();
  });

  it('should invoke', async () => {
    const spy = sinon.spy(async (_) => ({
      StatusCode: 200,
      ExecutedVersion: '$LATEST',
      Payload: '"Success"',
    }));
    mockLambda.on(InvokeCommand).callsFake(spy);

    const invoke = lambdaTest({ functionName: 'my-xyz-service-dev-listener' });

    const data = await invoke({ Records: [] });

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      FunctionName: 'my-xyz-service-dev-listener',
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify({ Records: [] }),
    });
    expect(data).to.deep.equal({
      StatusCode: 200,
      ExecutedVersion: '$LATEST',
      Payload: 'Success',
    });
  });
});
