/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, DynamoDB } from 'aws-sdk';
import Promise from 'bluebird';
import _ from 'highland';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    tableName,
    timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = debug;
    this.tableName = tableName || /* istanbul ignore next */ 'undefined';
    this.db = new DynamoDB.DocumentClient({
      httpOptions: {
        timeout,
        // agent: sslAgent,
      },
      logger: { log: /* istanbul ignore next */ (msg) => this.debug(msg) },
      convertEmptyValues: true,
    });
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

    return this.db.batchGet(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }

  query(inputParams) {
    const params = {
      TableName: this.tableName,
      ...inputParams,
    };

    let cursor;

    return _((push, next) => {
      params.ExclusiveStartKey = cursor;

      return this.db.query(params).promise()
        .tap(this.debug)
        .tapCatch(this.debug)
        .then((data) => {
          if (data.LastEvaluatedKey) {
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
}

export default Connector;
