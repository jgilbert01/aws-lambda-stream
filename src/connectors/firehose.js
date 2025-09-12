/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import Promise from 'bluebird';

import { omit, pick } from 'lodash';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
    timeout = Number(process.env.FIREHOSE_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.deliveryStreamName = deliveryStreamName || 'undefined';
    this.client = Connector.getClient(pipelineId, debug, timeout, additionalClientOpts);
    this.retryConfig = retryConfig;
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new FirehoseClient({
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

  putRecordBatch(inputParams, ctx) {
    const params = {
      DeliveryStreamName: this.deliveryStreamName,
      ...inputParams,
    };

    return this._putRecordBatch(params, [], ctx);
  }

  _putRecordBatch(params, attempts, ctx) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._sendCommand(new PutRecordBatchCommand(params))
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (resp.FailedPutCount > 0) {
            return this._putRecordBatch(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'firehose', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  Records: params.Records.filter((e, i) => resp.RequestResponses[i].ErrorCode),
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  RequestResponses: [
    ...c.RequestResponses.filter((r) => !r.ErrorCode),
    ...a.RequestResponses.filter((r) => !r.ErrorCode),
  ],
  attempts: [...attempts, resp],
}), resp);
