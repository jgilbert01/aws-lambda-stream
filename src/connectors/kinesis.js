/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import Promise from 'bluebird';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    streamName = process.env.STREAM_NAME,
    timeout = Number(process.env.KINESIS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    xrayEnabled = false,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.streamName = streamName || 'undefined';
    this.retryConfig = retryConfig;

    this.stream = Connector.getClient(pipelineId, debug, timeout);
    if (xrayEnabled) this.stream = require('../utils/xray').captureSdkClientTraces(this.stream);
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout) {
    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new KinesisClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
      });
    }
    return this.clients[pipelineId];
  }

  putRecords(inputParams) {
    const params = {
      StreamName: this.streamName,
      ...inputParams,
    };

    return this._putRecords(params, []);
  }

  _putRecords(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => Promise.resolve(this.stream.send(new PutRecordsCommand(params)))
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (resp.FailedRecordCount > 0) {
            return this._putRecords(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  Records: params.Records.filter((e, i) => resp.Records[i].ErrorCode),
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Records: [
    ...c.Records.filter((r) => !r.ErrorCode),
    ...a.Records.filter((r) => !r.ErrorCode),
  ],
  attempts: [...attempts, resp],
}), resp);
