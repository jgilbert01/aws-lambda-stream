/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
    timeout = Number(process.env.FIREHOSE_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    xrayEnabled = false,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.deliveryStreamName = deliveryStreamName || 'undefined';
    this.stream = new FirehoseClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      logger: defaultDebugLogger(debug),
    });
    if (xrayEnabled) this.stream = require('../utils/xray').captureSdkClientTraces(this.stream);
  }

  putRecordBatch(inputParams) {
    const params = {
      DeliveryStreamName: this.deliveryStreamName,
      ...inputParams,
    };

    return Promise.resolve(this.stream.send(new PutRecordBatchCommand(params)))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
