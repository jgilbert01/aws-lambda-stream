import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { mockClient } from 'aws-sdk-client-mock';
import {
  InvokeCommand,
  ListEventSourceMappingsCommand,
  CreateEventSourceMappingCommand,
  UpdateEventSourceMappingCommand,
  DeleteEventSourceMappingCommand,
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

  it('should list esm', async () => {
    const spy = sinon.spy((_) => ({
      UUID: '1',
      FunctionName: 'f1',
    }));
    mockLambda.on(ListEventSourceMappingsCommand).callsFake(spy);

    const params = {
      FunctionName: 'f1',
    };

    const data = await new Connector({ debug: debug('lambda') })
      .listEventSourceMappings(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      FunctionName: 'f1',
    });
    expect(data).to.deep.equal({
      UUID: '1',
      FunctionName: 'f1',
    });
  });

  it('should create esm', async () => {
    const spy = sinon.spy((_) => ({
      UUID: '1',
      EventSourceArn: 'esm1',
    }));
    mockLambda.on(CreateEventSourceMappingCommand).callsFake(spy);

    const params = {
      Enabled: false,
      BatchSize: 10,
    };

    const data = await new Connector({ debug: debug('lambda') })
      .createEventSourceMapping(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      Enabled: false,
      BatchSize: 10,
    });
    expect(data).to.deep.equal({
      UUID: '1',
      EventSourceArn: 'esm1',
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

  it('should delete esm', async () => {
    const spy = sinon.spy((_) => ({
      UUID: '1',
      EventSourceArn: 'esm1',
    }));
    mockLambda.on(DeleteEventSourceMappingCommand).callsFake(spy);

    const params = {
      UUID: '1',
    };

    const data = await new Connector({ debug: debug('lambda') })
      .deleteEventSourceMapping(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      UUID: '1',
    });
    expect(data).to.deep.equal({
      UUID: '1',
      EventSourceArn: 'esm1',
    });
  });
});
