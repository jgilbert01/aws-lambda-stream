import 'mocha';
import { expect } from 'chai';
import * as sinon from 'sinon';
import debug from 'debug';

import Connector, { updateExpression } from '../../../src/connectors/dynamodb';

const AWS = require('aws-sdk-mock');

AWS.Promise = Promise;

describe('connectors/dynamodb.js', () => {
  afterEach(() => {
    AWS.restore('DynamoDB.DocumentClient');
  });

  it('should update', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('DynamoDB.DocumentClient', 'update', spy);

    const data = await new Connector(debug('db'), 't1')
      .update({
        pk: '00000000-0000-0000-0000-000000000000',
        sk: 'thing',
      }, {
        name: 'thing0',
        timestamp: 1600051691001,
      });

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      TableName: 't1',
      Key: {
        pk: '00000000-0000-0000-0000-000000000000',
        sk: 'thing',
      },
      ExpressionAttributeNames: { '#name': 'name', '#timestamp': 'timestamp' },
      ExpressionAttributeValues: { ':name': 'thing0', ':timestamp': 1600051691001 },
      UpdateExpression: 'SET #name = :name, #timestamp = :timestamp',
      ReturnValues: 'ALL_NEW',
    });
    expect(data).to.deep.equal({});
  });

  it('should get by id', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {
      Items: [{
        pk: '00000000-0000-0000-0000-000000000000',
        sk: 'thing',
        name: 'thing0',
        timestamp: 1600051691001,
      }],
    }));

    AWS.mock('DynamoDB.DocumentClient', 'query', spy);

    const data = await new Connector(debug('db'), 't1')
      .get('00000000-0000-0000-0000-000000000000');

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      TableName: 't1',
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': '00000000-0000-0000-0000-000000000000' },
      ConsistentRead: true,
    });
    expect(data).to.deep.equal([{
      pk: '00000000-0000-0000-0000-000000000000',
      sk: 'thing',
      name: 'thing0',
      timestamp: 1600051691001,
    }]);
  });

  it('should calculate updateExpression', () => {
    expect(updateExpression({
      name: 'Thing One',
      description: 'This is thing one.',
      discriminator: 'thing',
      latched: true,
      ttl: 1543046400,
      timestamp: 1540454400000,
    })).to.deep.equal({
      ExpressionAttributeNames: {
        '#description': 'description',
        '#discriminator': 'discriminator',
        '#latched': 'latched',
        '#name': 'name',
        '#timestamp': 'timestamp',
        '#ttl': 'ttl',
      },
      ExpressionAttributeValues: {
        ':description': 'This is thing one.',
        ':discriminator': 'thing',
        ':latched': true,
        ':name': 'Thing One',
        ':timestamp': 1540454400000,
        ':ttl': 1543046400,
      },
      UpdateExpression: 'SET #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp',
      ReturnValues: 'ALL_NEW',
    });
  });
});
