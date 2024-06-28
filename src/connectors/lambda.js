/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    timeout = Number(process.env.LAMBDA_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    xrayEnabled = false,
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.opt = opt;

    this.client = Connector.getClient(pipelineId, debug, timeout);
    if (xrayEnabled) this.client = require('../metrics/xray').captureSdkClientTraces(this.client);
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout) {
    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new LambdaClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        logger: defaultDebugLogger(debug),
      });
    }
    return this.clients[pipelineId];
  }

  invoke(params, ctx) {
    return this._sendCommand(new InvokeCommand(params), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'lambda', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
