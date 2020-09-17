/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, DynamoDB } from 'aws-sdk';
import Promise from 'bluebird';
import merge from 'lodash/merge';
import _omit from 'lodash/omit';

config.setPromisesDependency(Promise);

export const updateExpression = Item => ({
  ExpressionAttributeNames: Object.keys(Item)
    .map(attrName => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {}),
  ExpressionAttributeValues: Object.keys(Item)
    .map(attrName => ({ [`:${attrName}`]: Item[attrName] }))
    .reduce(merge, {}),
  UpdateExpression: `SET ${Object.keys(Item)
    .map(attrName => `#${attrName} = :${attrName}`)
    .join(', ')}`,
  ReturnValues: 'ALL_NEW',
});

export const timestampCondition = (fieldName = 'timestamp') => ({
  ConditionExpression: `attribute_not_exists(#${fieldName}) OR #${fieldName} < :${fieldName}`,
});

const DEFAULT_OMIT_FIELDS = [
  'pk',
  'sk',
  'data',
  'discriminator',
  'ttl',
  'latched',
  'deleted',
  'aws:rep:updateregion',
  'aws:rep:updatetime',
  'aws:rep:deleting',
];

const DEFAULT_RENAME = { pk: 'id' };

export const mapper = ({
  defaults = {},
  rename = DEFAULT_RENAME,
  omit = DEFAULT_OMIT_FIELDS,
  transform = {},
} = {}) =>
  (o) => {
    const transformed = {
      ...o,
      ...Object.keys(transform).reduce((a, k) => {
        if (o[k]) a[k] = transform[k](o[k]);
        return a;
      }, {}),
    };

    const renamed = {
      ...o,
      ...Object.keys(rename).reduce((a, k) => {
        if (transformed[k]) a[rename[k]] = transformed[k];
        return a;
      }, {}),
    };

    return ({
      ...defaults,
      ..._omit(renamed, [...omit, ...Object.keys(rename)]),
    });
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
      logger: { log: /* istanbul ignore next */ msg => this.debug(msg) },
      convertEmptyValues: true,
    });
  }

  update(Key, inputParams, timestampFieldName = 'timestamp') {
    const params = {
      TableName: this.tableName,
      Key,
      ...updateExpression(inputParams),
      ...timestampCondition(timestampFieldName),
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

  get(id, mappings = mapper()) {
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
      .then(data => data.Items.map(mappings));
  }
}

export default Connector;
