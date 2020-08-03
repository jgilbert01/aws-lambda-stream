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
    this.debug = debug;
    this.topicArn = topicArn || 'undefined';
    this.topic = new SNS({
      httpOptions: {
        timeout,
        // agent: sslAgent,
      },
      logger: { log: /* istanbul ignore next */ (msg) => this.debug(msg) },
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
