/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { TimestreamWriteClient, WriteRecordsCommand } from '@aws-sdk/client-timestream-write';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { omit, pick } from 'lodash';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    timeout = Number(process.env.CW_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.client = Connector.getClient(pipelineId, debug, timeout, additionalClientOpts); this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new TimestreamWriteClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
          ...addlRequestHandlerOpts,
        }),
        logger: defaultDebugLogger(debug),
        ...addlClientOpts,
      });
    }
    return this.clients[pipelineId];
  }

  writeRecords(params, ctx) {
    return this._sendCommand(new WriteRecordsCommand(params), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'timestream', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
