/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import Promise from 'bluebird';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.BUS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bus = new EventBridgeClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
      logger: defaultDebugLogger(debug),
    });
    this.retryConfig = retryConfig;
  }

  putEvents(params) {
    return this._putEvents(params, []);
  }

  _putEvents(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => Promise.resolve(this.bus.send(new PutEventsCommand(params)))
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
