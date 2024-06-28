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
    pipelineId,
    removeUndefinedValues = true,
    timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    retryConfig = defaultRetryConfig,
    xrayEnabled = false,
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.tableName = tableName || /* istanbul ignore next */ 'undefined';
    this.retryConfig = retryConfig;
    this.opt = opt;

    this.client = Connector.getClient(pipelineId, debug, convertEmptyValues, removeUndefinedValues, timeout);
    if (xrayEnabled) this.client = require('../metrics/xray').captureSdkClientTraces(this.client, true);
  }

  static clients = {};

  static getClient(pipelineId, debug, convertEmptyValues, removeUndefinedValues, timeout) {
    if (!this.clients[pipelineId]) {
      const dynamoClient = new DynamoDBClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
      });
      this.clients[pipelineId] = DynamoDBDocumentClient.from(dynamoClient, {
        marshallOptions: {
          convertEmptyValues,
          removeUndefinedValues,
        },
      });
    }
    return this.clients[pipelineId];
  }

  update(inputParams, ctx) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new UpdateCommand(params), ctx)
      .catch((err) => {
        /* istanbul ignore else */
        if (err.name === 'ConditionalCheckFailedException') {
          return {};
        }
        /* istanbul ignore next */
        return Promise.reject(err);
      });
  }

  put(inputParams, ctx) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new PutCommand(params), ctx);
  }

  batchGet(inputParams, ctx) {
    const params = {
      ...inputParams,
    };

    return this._batchGet(params, [], ctx);
  }

  query(inputParams, ctx) {
    return this.queryAll(inputParams, ctx);
  }

  queryAll(inputParams, ctx) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    let cursor;
    let itemsCount = 0;

    return _((push, next) => {
      params.ExclusiveStartKey = cursor;
      return this._executeCommand(new QueryCommand(params), ctx)
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

  queryPage(inputParams, ctx) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new QueryCommand(params), ctx);
  }

  scan(inputParams, ctx) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    return this._executeCommand(new ScanCommand(params), ctx);
  }

  _batchGet(params, attempts, ctx) {
    assertMaxRetries(attempts, this.retryConfig.maxRetries);

    return wait(getDelay(this.retryConfig.retryWait, attempts.length))
      .then(() => this._executeCommand(new BatchGetCommand(params), ctx)
        .then((resp) => {
          const response = {
            Responses: {},
            ...resp,
          };
          if (Object.keys(response.UnprocessedKeys || /* istanbul ignore next */ {}).length > 0) {
            return this._batchGet(unprocessed(params, response), [...attempts, response], ctx);
          } else {
            return accumlate(attempts, response);
          }
        }));
  }

  _executeCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'dynamodb', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
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
