/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
    timeout = Number(process.env.FIREHOSE_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.deliveryStreamName = deliveryStreamName || 'undefined';
    this.client = Connector.getClient(pipelineId, debug, timeout);
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout) {
    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new FirehoseClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        logger: defaultDebugLogger(debug),
      });
    }
    return this.clients[pipelineId];
  }

  putRecordBatch(inputParams, ctx) {
    const params = {
      DeliveryStreamName: this.deliveryStreamName,
      ...inputParams,
    };

    return this._sendCommand(new PutRecordBatchCommand(params), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'firehose', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
