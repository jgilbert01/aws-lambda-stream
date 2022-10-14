/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { SNS, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    topicArn = process.env.TOPIC_ARN,
    timeout = Number(process.env.SNS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%o', msg);
    this.topicArn = topicArn || 'undefined';
    this.topic = new SNS({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg) },
    });
  }

  publish(inputParams) {
    const params = {
      TopicArn: this.topicArn,
      ...inputParams,
    };

    return this.topic.publish(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
