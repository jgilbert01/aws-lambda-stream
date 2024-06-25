/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */

import Promise from 'bluebird';

import { PublishBatchCommand, PublishCommand, SNSClient } from '@aws-sdk/client-sns';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    topicArn = process.env.TOPIC_ARN,
    timeout = Number(process.env.SNS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.topicArn = topicArn || 'undefined';
    this.client = new SNSClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
      logger: defaultDebugLogger(debug),
    });
    this.retryConfig = retryConfig;
    this.opt = opt;
  }

  publish(inputParams, ctx) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this._sendCommand(new PublishCommand(params), ctx);
  }

  publishBatch(inputParams, ctx) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this._publishBatch(params, [], ctx);
  }

  _publishBatch(params, attempts, ctx) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._sendCommand(new PublishBatchCommand(params), ctx)
        .then((resp) => {
          if (resp.Failed?.length > 0) {
            return this._publishBatch(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'sns', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  PublishBatchRequestEntries: resp.Failed.map((m) => params.PublishBatchRequestEntries.find((e) => m.Id === e.Id)),
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Successful: [
    ...c.Successful,
    ...a.Successful,
  ],
  attempts: [...attempts, resp],
}), resp);
