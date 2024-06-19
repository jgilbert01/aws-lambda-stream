/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import {
  InvokeCommand,
  UpdateEventSourceMappingCommand,
  LambdaClient,
} from '@aws-sdk/client-lambda';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.LAMBDA_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.client = new LambdaClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      logger: defaultDebugLogger(debug),
    });
  }

  invoke(params) {
    return this._sendCommand(new InvokeCommand(params));
  }

  updateEventSourceMapping(params) {
    return this._sendCommand(new UpdateEventSourceMappingCommand(params));
  }

  _sendCommand(command) {
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
