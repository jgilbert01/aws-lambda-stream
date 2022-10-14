/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { SQS, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    queueUrl = process.env.QUEUE_URL,
    timeout = Number(process.env.SQS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%o', msg);
    this.queueUrl = queueUrl || 'undefined';
    this.queue = new SQS({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg) },
    });
  }

  sendMessageBatch(inputParams) {
    const params = {
      QueueUrl: this.queueUrl,
      ...inputParams,
    };

    return this.queue.sendMessageBatch(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
