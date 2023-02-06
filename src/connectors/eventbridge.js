/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { EventBridge, config } from 'aws-sdk';
import Promise from 'bluebird';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries,
} from '../utils/retry';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.BUS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bus = new EventBridge({
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

  putEvents(params) {
    return this._putEvents(params, []);
  }

  _putEvents(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this.bus.putEvents(params)
        .promise()
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (resp.FailedEntryCount > 0) {
            return this._putEvents(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  Entries: params.Entries.filter((e, i) => resp.Entries[i].ErrorCode),
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Entries: [
    ...c.Entries.filter((e) => !e.ErrorCode),
    ...a.Entries.filter((e) => !e.ErrorCode),
  ],
  attempts: [...attempts, resp],
}), resp);
