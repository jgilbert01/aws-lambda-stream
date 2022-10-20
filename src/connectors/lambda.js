/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, Lambda } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.LAMBDA_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.lambda = new Lambda({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  invoke(params) {
    return this.lambda.invoke(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
