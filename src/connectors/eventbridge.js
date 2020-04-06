/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { EventBridge, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    timeout = process.env.BUS_TIMEOUT || process.env.TIMEOUT || 1000,
  }) {
    this.debug = debug;
    this.bus = new EventBridge({
      httpOptions: {
        timeout,
        // agent: sslAgent,
      },
      logger: { log: /* istanbul ignore next */ (msg) => this.debug(msg) },
    });
  }

  putEvents(params) {
    return this.bus.putEvents(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
