import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import Promise from 'bluebird';

import Connector from '../../../src/connectors/dynamodb';

import { debug } from '../../../src/utils';

const AWS = require('aws-sdk-mock');

AWS.Promise = Promise;

describe('connectors/dynamodb.js', () => {
  afterEach(() => {
    AWS.restore('DynamoDB.DocumentClient');
  });

  it('should update', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('DynamoDB.DocumentClient', 'update', spy);

    const UPDATE_REQUEST = {
      // TableName: 'my-service-entities',
      Key: {
        pk: '1',
        sk: 'thing',
      },
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
        '#description': 'description',
        '#discriminator': 'discriminator',
        '#latched': 'latched',
        '#ttl': 'ttl',
        '#timestamp': 'timestamp',
      },
      ExpressionAttributeValues: {
        ':id': '1',
        ':name': 'Thing One',
        ':description': 'This is thing 1',
        ':discriminator': 'thing',
        ':latched': true,
        ':ttl': 1549053422,
        ':timestamp': 1548967022000,
      },
      UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp',
      ReturnValues: 'ALL_NEW',

      ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'my-service-entities',
    })
      .update(UPDATE_REQUEST);

    expect(spy).to.have.been.calledWith({
      TableName: 'my-service-entities',
      Key: { pk: '1', sk: 'thing' },
      ExpressionAttributeNames: {
        '#description': 'description',
        '#discriminator': 'discriminator',
        '#id': 'id',
        '#latched': 'latched',
        '#name': 'name',
        '#timestamp': 'timestamp',
        '#ttl': 'ttl',
      },
      ExpressionAttributeValues: {
        ':description': 'This is thing 1',
        ':discriminator': 'thing',
        ':id': '1',
        ':latched': true,
        ':name': 'Thing One',
        ':timestamp': 1548967022000,
        ':ttl': 1549053422,
      },
      UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp',
      ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
      ReturnValues: 'ALL_NEW',
    });
    expect(data).to.deep.equal({});
  });
});
