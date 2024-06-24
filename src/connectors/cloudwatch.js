/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.CW_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    xrayEnabled = false,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.cw = new CloudWatchClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      logger: defaultDebugLogger(debug),
    });
    if (xrayEnabled) this.cw = require('../utils/xray').captureSdkClientTraces(this.cw);
  }

  put({ Namespace, MetricData }) {
    const params = {
      Namespace,
      MetricData,
    };

    const command = new PutMetricDataCommand(params);
    return Promise.resolve(this.cw.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
