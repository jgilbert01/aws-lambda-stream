/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */

import Promise from 'bluebird';

import {
  ApiGatewayManagementApiClient,
  DeleteConnectionCommand,
  GetConnectionCommand,
  PostToConnectionCommand,
} from '@aws-sdk/client-apigatewaymanagementapi';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import { omit, pick } from 'lodash';
import { defaultBackoffDelay } from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    endpoint = process.env.WEBSOCKET_ENDPOINT,
    timeout = Number(process.env.WS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.endpoint = endpoint || 'undefined';
    this.client = Connector.getClient(pipelineId, debug, timeout, endpoint, additionalClientOpts);
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, endpoint, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new ApiGatewayManagementApiClient({
        endpoint,
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
          ...addlRequestHandlerOpts,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
        ...addlClientOpts,
      });
    }
    return this.clients[pipelineId];
  }

  postToConnection(connectionId, data, ctx) {
    const params = {
      ConnectionId: connectionId,
      Data: typeof data === 'string' ? data : JSON.stringify(data),
    };

    return this._sendCommand(new PostToConnectionCommand(params), ctx);
  }

  getConnection(connectionId, ctx) {
    return this._sendCommand(new GetConnectionCommand({ ConnectionId: connectionId }), ctx);
  }

  deleteConnection(connectionId, ctx) {
    return this._sendCommand(new DeleteConnectionCommand({ ConnectionId: connectionId }), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'ws', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
