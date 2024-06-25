/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchGetCommand,
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import Promise from 'bluebird';
import _ from 'highland';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import {
  defaultRetryConfig, wait, getDelay, assertMaxRetries, defaultBackoffDelay,
} from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    tableName,
    convertEmptyValues,
    removeUndefinedValues = true,
    timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    xrayEnabled = false,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.tableName = tableName || /* istanbul ignore next */ 'undefined';
    const dynamoClient = new DynamoDBClient({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
      logger: defaultDebugLogger(debug),
    });
    this.db = DynamoDBDocumentClient.from(dynamoClient, {
      marshallOptions: {
        convertEmptyValues,
        removeUndefinedValues,
      },
    });
    if (xrayEnabled) this.db = require('../utils/xray').captureSdkClientTraces(this.db, true);

    this.retryConfig = retryConfig;
  }

  update(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new UpdateCommand(params))
      .catch((err) => {
        /* istanbul ignore else */
        if (err.name === 'ConditionalCheckFailedException') {
          return {};
        }
        /* istanbul ignore next */
        return Promise.reject(err);
      });
  }

  put(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new PutCommand(params));
  }

  batchGet(inputParams) {
    const params = {
      ...inputParams,
    };

    return this._batchGet(params, []);
  }

  query(inputParams) {
    return this.queryAll(inputParams);
  }

  queryAll(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    let cursor;
    let itemsCount = 0;

    return _((push, next) => {
      params.ExclusiveStartKey = cursor;
      return this._executeCommand(new QueryCommand(params))
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

  queryPage(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new QueryCommand(params));
  }

  scan(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new ScanCommand(params));
  }

  _batchGet(params, attempts) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._executeCommand(new BatchGetCommand(params))
        .then((resp) => {
          const response = {
            Responses: {},
            ...resp,
          };
          if (Object.keys(response.UnprocessedKeys || /* istanbul ignore next */ {}).length > 0) {
            return this._batchGet(unprocessed(params, response), [...attempts, response]);
          } else {
            return accumlate(attempts, response);
          }
        }));
  }

  _executeCommand(command) {
    return Promise.resolve(this.db.send(command))
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
    [c2]: [...(a2[c2] || []), ...a.Responses[c2]],
  }), { ...c.Responses }),
  attempts: [...attempts, resp],
}), resp);
