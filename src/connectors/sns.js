/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { SNS, config } from 'aws-sdk';
import Promise from 'bluebird';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries,
} from '../utils/retry';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    topicArn = process.env.TOPIC_ARN,
    timeout = Number(process.env.SNS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.topicArn = topicArn || 'undefined';
    this.topic = new SNS({
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

  publish(inputParams) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this.topic.publish(params)
      .promise()
      .tap(this.debug)
      .tapCatch(this.debug);
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
      .then(() => this.topic.publishBatch(params)
        .promise()
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (resp.Failed?.length > 0) {
            return this._publishBatch(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
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
