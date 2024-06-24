/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.LAMBDA_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    xrayEnabled = process.env.XRAY_ENABLED === 'true',
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.lambda = new LambdaClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      logger: defaultDebugLogger(debug),
    });
    if (xrayEnabled) this.lambda = require('../utils/xray').captureSdkClientTraces(this.lambda);
  }

  invoke(params) {
    return Promise.resolve(this.lambda.send(new InvokeCommand(params)))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
