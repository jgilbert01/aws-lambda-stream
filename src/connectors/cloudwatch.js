/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    timeout = Number(process.env.CW_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.opt = opt;
    this.client = Connector.getClient(pipelineId, debug, timeout);
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout) {
    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new CloudWatchClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        logger: defaultDebugLogger(debug),
      });
    }
    return this.clients[pipelineId];
  }

  put({ Namespace, MetricData }, ctx) {
    const params = {
      Namespace,
      MetricData,
    };

    const command = new PutMetricDataCommand(params);
    return this._executeCommand(command, ctx);
  }

  _executeCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'cloudwatch', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
