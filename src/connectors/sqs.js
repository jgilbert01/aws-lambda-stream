/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { SQS, config } from 'aws-sdk';
import Promise from 'bluebird';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries,
} from '../utils/retry';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    queueUrl = process.env.QUEUE_URL,
    timeout = Number(process.env.SQS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.queueUrl = queueUrl || 'undefined';
    this.queue = new SQS({
      httpOptions: {
        timeout,
        connectTimeout: timeout,
      },
      maxRetries: 10, // Default: 3
      retryDelayOptions: { base: 200 }, // Default: 100 ms
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
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
      .then(() => this.queue.sendMessageBatch(params)
        .promise()
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

export const unprocessed = (params, resp) => ({
  ...params,
  Entries: resp.Failed.map((m) => params.Entries.find((e) => m.Id === e.Id)),
});

export const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Successful: [
    ...c.Successful,
    ...a.Successful,
  ],
  attempts: [...attempts, resp],
}), resp);
