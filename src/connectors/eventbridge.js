/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { EventBridge, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.BUS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bus = new EventBridge({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  putEvents(params) {
    return this.bus.putEvents(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
