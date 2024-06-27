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
    pipelineId,
    topicArn = process.env.TOPIC_ARN,
    timeout = Number(process.env.SNS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.topicArn = topicArn || 'undefined';
    this.topic = Connector.getClient(pipelineId, debug, timeout);
    this.retryConfig = retryConfig;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout) {
    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new SNSClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
      });
    }
    return this.clients[pipelineId];
  }

  publish(inputParams) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this._sendCommand(new PublishCommand(params));
  }

  publishBatch(inputParams) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this._publishBatch(params, []);
  }

  _publishBatch(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._sendCommand(new PublishBatchCommand(params))
        .then((resp) => {
          if (resp.Failed?.length > 0) {
            return this._publishBatch(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }

  _sendCommand(command) {
    return Promise.resolve(this.topic.send(command))
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
