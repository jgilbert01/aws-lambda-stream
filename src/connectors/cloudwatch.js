/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, CloudWatch } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.CW_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%o', msg);
    this.cw = new CloudWatch({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg) },
    });
  }

  put({ Namespace, MetricData }) {
    const params = {
      Namespace,
      MetricData,
    };

    return this.cw.putMetricData(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
