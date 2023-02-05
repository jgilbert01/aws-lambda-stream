/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { Kinesis, config } from 'aws-sdk';
import Promise from 'bluebird';

import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries,
} from '../utils/retry';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    streamName = process.env.STREAM_NAME,
    timeout = Number(process.env.KINESIS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.streamName = streamName || 'undefined';
    this.stream = new Kinesis({
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
      .then(() => this.stream.putRecords(params)
        .promise()
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

export const unprocessed = (params, resp) => ({
  ...params,
  Records: params.Records.filter((e, i) => resp.Records[i].ErrorCode),
});

export const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Records: [
    ...c.Records.filter((r) => !r.ErrorCode),
    ...a.Records.filter((r) => !r.ErrorCode),
  ],
  attempts: [...attempts, resp],
}), resp);
