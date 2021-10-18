/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, DynamoDB } from 'aws-sdk';
import Promise from 'bluebird';
import merge from 'lodash/merge';

config.setPromisesDependency(Promise);

export const updateExpression = (Item) => {
  const keys = Object.keys(Item);

  const ExpressionAttributeNames = keys
    .filter((attrName) => Item[attrName] !== undefined)
    .map((attrName) => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {});

  const ExpressionAttributeValues = keys
    .filter((attrName) => Item[attrName] !== undefined && Item[attrName] !== null)
    .map((attrName) => ({ [`:${attrName}`]: Item[attrName] }))
    .reduce(merge, {});

  let UpdateExpression = `SET ${keys
    .filter((attrName) => Item[attrName] !== undefined && Item[attrName] !== null)
    .map((attrName) => `#${attrName} = :${attrName}`)
    .join(', ')}`;

  const UpdateExpressionRemove = keys
    .filter((attrName) => Item[attrName] === null)
    .map((attrName) => `#${attrName}`)
    .join(', ');

  if (UpdateExpressionRemove.length) {
    UpdateExpression = `${UpdateExpression} REMOVE ${UpdateExpressionRemove}`;
  }

  return {
    ExpressionAttributeNames,
    ExpressionAttributeValues,
    UpdateExpression,
    ReturnValues: 'ALL_NEW',
  };
};

class Connector {
  constructor(
    debug,
    tableName,
    timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  ) {
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

  update(Key, inputParams) {
    const params = {
      TableName: this.tableName,
      Key,
      ...updateExpression(inputParams),
    };

    return this.db.update(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }

  get(id) {
    const params = {
      TableName: this.tableName,
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: {
        '#pk': 'pk',
      },
      ExpressionAttributeValues: {
        ':pk': id,
      },
      ConsistentRead: true,
    };

    return this.db.query(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug)
      .then((data) => data.Items);
    // TODO assert data.LastEvaluatedKey
  }
}

export default Connector;
