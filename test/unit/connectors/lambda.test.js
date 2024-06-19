import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { mockClient } from 'aws-sdk-client-mock';
import {
  InvokeCommand,
  UpdateEventSourceMappingCommand,
  LambdaClient,
} from '@aws-sdk/client-lambda';

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

  it('should update esm', async () => {
    const spy = sinon.spy((_) => ({
      UUID: '1',
      EventSourceArn: 'esm1',
    }));
    mockLambda.on(UpdateEventSourceMappingCommand).callsFake(spy);

    const params = {
      UUID: '1',
      Enabled: false,
      BatchSize: 10,
    };

    const data = await new Connector({ debug: debug('lambda') })
      .updateEventSourceMapping(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      UUID: '1',
      Enabled: false,
      BatchSize: 10,
    });
    expect(data).to.deep.equal({
      UUID: '1',
      EventSourceArn: 'esm1',
    });
  });
});
