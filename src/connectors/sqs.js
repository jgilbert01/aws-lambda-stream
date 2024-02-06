/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import Promise from 'bluebird';
import { SQSClient, SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    queueUrl = process.env.QUEUE_URL,
    timeout = Number(process.env.SQS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.queueUrl = queueUrl || 'undefined';
    this.queue = new SQSClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
      logger: defaultDebugLogger(debug),
    });
    this.retryConfig = retryConfig;
  }

  sendMessageBatch(inputParams) {
    const params = {
      QueueUrl: this.queueUrl,
      ...inputParams,
    };

    return this._sendMessageBatch(params, []);
  }

  _sendMessageBatch(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => Promise.resolve(this.queue.send(new SendMessageBatchCommand(params)))
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (resp.Failed?.length > 0) {
            return this._sendMessageBatch(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  Entries: resp.Failed.map((m) => params.Entries.find((e) => m.Id === e.Id)),
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Successful: [
    ...c.Successful,
    ...a.Successful,
  ],
  attempts: [...attempts, resp],
}), resp);
