/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import Promise from 'bluebird';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import { omit, pick } from 'lodash';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    timeout = Number(process.env.BUS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.client = Connector.getClient(pipelineId, debug, timeout, additionalClientOpts);
    this.retryConfig = retryConfig;
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new EventBridgeClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
          ...addlRequestHandlerOpts,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
        ...addlClientOpts,
      });
    }
    return this.clients[pipelineId];
  }

  putEvents(params, ctx) {
    return this._putEvents(params, [], ctx);
  }

  _putEvents(params, attempts, ctx) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._sendCommand(new PutEventsCommand(params), ctx)
        .then((resp) => {
          if (resp.FailedEntryCount > 0) {
            return this._putEvents(unprocessed(params, resp), [...attempts, resp], ctx);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'eventbridge', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
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
