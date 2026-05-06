import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';
import {
  ApiGatewayManagementApiClient,
  DeleteConnectionCommand,
  GetConnectionCommand,
  PostToConnectionCommand,
} from '@aws-sdk/client-apigatewaymanagementapi';

import Connector from '../../../src/connectors/apigatewayclient';

import { debug } from '../../../src/utils';

describe('connectors/websocket.js', () => {
  let mockWs = mockClient(ApiGatewayManagementApiClient);

  beforeEach(() => {
    mockWs = mockClient(ApiGatewayManagementApiClient);
  });

  afterEach(() => {
    mockWs.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('ws-test1', debug('test'), 1000, 'https://test');
    const client2 = Connector.getClient('ws-test1', debug('test'), 1000, 'https://test');
    const client3 = Connector.getClient('ws-test2', debug('test'), 1000, 'https://test');

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should post to connection with object data', async () => {
    const spy = sinon.spy((_) => ({}));
    mockWs.on(PostToConnectionCommand).callsFake(spy);

    const data = await new Connector({
      debug: debug('ws'),
      endpoint: 'https://test.execute-api.us-west-2.amazonaws.com/dev',
    }).postToConnection('conn-1', { type: 'thing-updated', data: { id: '1' } });

    expect(spy).to.have.been.calledWith({
      ConnectionId: 'conn-1',
      Data: JSON.stringify({ type: 'thing-updated', data: { id: '1' } }),
    });
    expect(data).to.deep.equal({});
  });

  it('should post to connection with string data', async () => {
    const spy = sinon.spy((_) => ({}));
    mockWs.on(PostToConnectionCommand).callsFake(spy);

    const data = await new Connector({
      debug: debug('ws'),
      endpoint: 'https://test.execute-api.us-west-2.amazonaws.com/dev',
    }).postToConnection('conn-1', 'raw string');

    expect(spy).to.have.been.calledWith({
      ConnectionId: 'conn-1',
      Data: 'raw string',
    });
    expect(data).to.deep.equal({});
  });

  it('should get connection', async () => {
    const spy = sinon.spy((_) => ({ ConnectedAt: '2024-01-01T00:00:00Z', LastActiveAt: '2024-01-01T01:00:00Z' }));
    mockWs.on(GetConnectionCommand).callsFake(spy);

    const data = await new Connector({
      debug: debug('ws'),
      endpoint: 'https://test.execute-api.us-west-2.amazonaws.com/dev',
    }).getConnection('conn-1');

    expect(spy).to.have.been.calledWith({ ConnectionId: 'conn-1' });
    expect(data).to.deep.equal({ ConnectedAt: '2024-01-01T00:00:00Z', LastActiveAt: '2024-01-01T01:00:00Z' });
  });

  it('should delete connection', async () => {
    const spy = sinon.spy((_) => ({}));
    mockWs.on(DeleteConnectionCommand).callsFake(spy);

    const data = await new Connector({
      debug: debug('ws'),
      endpoint: 'https://test.execute-api.us-west-2.amazonaws.com/dev',
    }).deleteConnection('conn-1');

    expect(spy).to.have.been.calledWith({ ConnectionId: 'conn-1' });
    expect(data).to.deep.equal({});
  });
});
