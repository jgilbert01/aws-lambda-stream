/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, DynamoDB } from 'aws-sdk';
import Promise from 'bluebird';
import _ from 'highland';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries,
} from '../utils/retry';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    tableName,
    timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.tableName = tableName || /* istanbul ignore next */ 'undefined';
    this.db = new DynamoDB.DocumentClient({
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

  update(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this.db.update(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug)
      .catch(/* istanbul ignore next */(err) => {
        if (err.code === 'ConditionalCheckFailedException') {
          return {};
        }
        return Promise.reject(err);
      });
  }

  put(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this.db.put(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }

  batchGet(inputParams) {
    const params = {
      ...inputParams,
    };

    return this._batchGet(params, []);
  }

  _batchGet(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this.db.batchGet(params)
        .promise()
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((resp) => {
          if (Object.keys(resp.UnprocessedKeys || /* istanbul ignore next */ {}).length > 0) {
            return this._batchGet(unprocessed(params, resp), [...attempts, resp]);
          } else {
            return accumlate(attempts, resp);
          }
        }));
  }

  query(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    let cursor;
    let itemsCount = 0;

    return _((push, next) => {
      params.ExclusiveStartKey = cursor;
      return this.db.query(params).promise()
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((data) => {
          itemsCount += data.Items.length;

          if (data.LastEvaluatedKey && (!params.Limit || (params.Limit && itemsCount < params.Limit))) {
            cursor = data.LastEvaluatedKey;
          } else {
            cursor = undefined;
          }

          data.Items.forEach((obj) => {
            push(null, obj);
          });
        })
        .catch(/* istanbul ignore next */(err) => {
          push(err, null);
        })
        .finally(() => {
          if (cursor) {
            next();
          } else {
            push(null, _.nil);
          }
        });
    })
      .collect()
      .toPromise(Promise);
  }

  scan(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this.db.scan(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;

const unprocessed = (params, resp) => ({
  ...params,
  RequestItems: resp.UnprocessedKeys,
});

const accumlate = (attempts, resp) => attempts.reduceRight((a, c) => ({
  ...a,
  Responses: Object.keys(a.Responses).reduce((a2, c2) => ({
    ...a2,
    [c2]: [...a2[c2], ...a.Responses[c2]],
  }), { ...c.Responses }),
  attempts: [...attempts, resp],
}), resp);
