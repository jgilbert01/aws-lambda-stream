import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/dynamodb';

import { debug } from '../../../src/utils';

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

  it('should put', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('DynamoDB.DocumentClient', 'put', spy);

    const PUT_REQUEST = {
      // TableName: 'my-service-entities',
      Item: {
        pk: '1',
        sk: 'thing',
        name: 'Thing One',
        description: 'This is thing 1',
        discriminator: 'thing',
        latched: true,
        ttl: 1549053422,
        timestamp: 1548967022000,
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'my-service-entities',
    })
      .put(PUT_REQUEST);

    expect(spy).to.have.been.calledWith({
      TableName: 'my-service-entities',
      Item: {
        pk: '1',
        sk: 'thing',
        name: 'Thing One',
        description: 'This is thing 1',
        discriminator: 'thing',
        latched: true,
        ttl: 1549053422,
        timestamp: 1548967022000,
      },
    });
    expect(data).to.deep.equal({});
  });

  it('should batchGet', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {
      Responses: {
        'stg-my-service-events': [
          {
            'aws:rep:deleting': false,
            'timestamp': 1548967022000,
            'sk': 'thing',
            'discriminator': 'thing',
            'aws:rep:updateregion': 'us-east-1',
            'latched': true,
            'aws:rep:updatetime': 1625157459.122001,
            'description': 'This is thing 1',
            'pk': '1',
            'name': 'Thing One',
          },
        ],
      },
      UnprocessedKeys: {},
    }));
    AWS.mock('DynamoDB.DocumentClient', 'batchGet', spy);

    const GET_REQUEST = {
      RequestItems: {
        'stg-my-service-events': {
          Keys: [{
            pk: '1',
            sk: 'thing',
          }],
        },
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'stg-my-service-events',
    })
      .batchGet(GET_REQUEST);

    expect(spy).to.have.been.calledWith({
      RequestItems: {
        'stg-my-service-events': {
          Keys: [{
            pk: '1',
            sk: 'thing',
          }],
        },
      },
    });
    expect(data).to.deep.equal({
      Responses: {
        'stg-my-service-events': [
          {
            'aws:rep:deleting': false,
            'timestamp': 1548967022000,
            'sk': 'thing',
            'discriminator': 'thing',
            'aws:rep:updateregion': 'us-east-1',
            'latched': true,
            'aws:rep:updatetime': 1625157459.122001,
            'description': 'This is thing 1',
            'pk': '1',
            'name': 'Thing One',
          },
        ],
      },
      UnprocessedKeys: {},
    });
  });

  it('should query', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((params, cb) => cb(null, {
      Items: [{
        pk: params.ExclusiveStartKey === undefined ? '1' : '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
      LastEvaluatedKey: params.ExclusiveStartKey === undefined ? '1' : undefined,
    }));

    AWS.mock('DynamoDB.DocumentClient', 'query', spy);

    const QUERY_REQUEST = {
      IndexName: 'DataIndex',
      KeyConditionExpression: '#data = :data',
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': correlationKey,
      },
      ConsistentRead: true,
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'my-service-entities',
    })
      .query(QUERY_REQUEST);

    expect(spy).to.have.been.calledWith({
      TableName: 'my-service-entities',
      IndexName: 'DataIndex',
      KeyConditionExpression: '#data = :data',
      ExpressionAttributeNames: { '#data': 'data' },
      ExpressionAttributeValues: { ':data': '11' },
      ConsistentRead: true,
      ExclusiveStartKey: '1',
    });
    expect(data).to.deep.equal([
      {
        pk: '1',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      },
      {
        pk: '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      },
    ]);
  });
});
