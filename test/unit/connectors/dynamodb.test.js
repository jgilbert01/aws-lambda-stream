import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import {
  BatchGetCommand,
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  ConditionalCheckFailedException,
} from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { debug } from '../../../src/utils';
import Connector from '../../../src/connectors/dynamodb';

describe('connectors/dynamodb.js', () => {
  let mockDdb;

  beforeEach(() => {
    mockDdb = mockClient(DynamoDBDocumentClient);
  });

  afterEach(() => {
    mockDdb.restore();
  });

  it('should update', async () => {
    const spy = sinon.spy((_) => ({}));
    mockDdb.on(UpdateCommand).callsFake(spy);

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

  it('should raise and catch oplock', async () => {
    mockDdb.on(UpdateCommand).rejects(new ConditionalCheckFailedException({}));

    const UPDATE_REQUEST = {
      Key: {
        pk: '1',
        sk: 'thing',
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'my-service-entities',
    })
      .update(UPDATE_REQUEST);

    expect(data).to.deep.equal({});
  });

  it('should put', async () => {
    const spy = sinon.spy((_) => ({}));
    mockDdb.on(PutCommand).callsFake(spy);

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
    const spy = sinon.spy((_) => ({
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
    mockDdb.on(BatchGetCommand).callsFake(spy);

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

  it('should retry', async () => {
    const responses = [
      {
        Responses: {
          t1: [
            {
              pk: '1',
              sk: 'thing',
              name: 'Thing One',
            },
          ],
        },
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '2',
                sk: 'thing',
              },
              {
                pk: '3',
                sk: 'thing',
              },
            ],
          },
        },
      },
      {
        Responses: {
          t1: [
            {
              pk: '2',
              sk: 'thing',
              name: 'Thing Two',
            },
          ],
        },
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '3',
                sk: 'thing',
              },
            ],
          },
        },
      },
      {
        Responses: {
          t1: [
            {
              pk: '3',
              sk: 'thing',
              name: 'Thing Three',
            },
          ],
        },
        UnprocessedKeys: {},
      },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockDdb.on(BatchGetCommand).callsFake(spy);

    const inputParams = {
      RequestItems: {
        t1: {
          Keys: [
            {
              pk: '1',
              sk: 'thing',
            },
            {
              pk: '2',
              sk: 'thing',
            },
            {
              pk: '3',
              sk: 'thing',
            },
          ],
        },
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
    }).batchGet(inputParams);

    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[0], inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });
    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });
    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });

    expect(data).to.deep.equal({
      Responses: {
        t1: [
          {
            pk: '1',
            sk: 'thing',
            name: 'Thing One',
          },
          {
            pk: '2',
            sk: 'thing',
            name: 'Thing Two',
          },
          {
            pk: '3',
            sk: 'thing',
            name: 'Thing Three',
          },
        ],
      },
      UnprocessedKeys: {},
      attempts: [
        {
          Responses: {
            t1: [
              {
                pk: '1',
                sk: 'thing',
                name: 'Thing One',
              },
            ],
          },
          UnprocessedKeys: {
            t1: {
              Keys: [
                {
                  pk: '2',
                  sk: 'thing',
                },
                {
                  pk: '3',
                  sk: 'thing',
                },
              ],
            },
          },
        },
        {
          Responses: {
            t1: [
              {
                pk: '2',
                sk: 'thing',
                name: 'Thing Two',
              },
            ],
          },
          UnprocessedKeys: {
            t1: {
              Keys: [
                {
                  pk: '3',
                  sk: 'thing',
                },
              ],
            },
          },
        },
        {
          Responses: {
            t1: [
              {
                pk: '3',
                sk: 'thing',
                name: 'Thing Three',
              },
            ],
          },
          UnprocessedKeys: {},
        },
      ],
    });
  });

  it('should retry when missing Responses', async () => {
    const responses = [
      {
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '4',
                sk: 'thing',
              },
              {
                pk: '5',
                sk: 'thing',
              },
              {
                pk: '6',
                sk: 'thing',
              },
            ],
          },
        },
      },
      {
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '4',
                sk: 'thing',
              },
              {
                pk: '5',
                sk: 'thing',
              },
              {
                pk: '6',
                sk: 'thing',
              },
            ],
          },
        },
      },
      {
        Responses: {
          t1: [
            {
              pk: '4',
              sk: 'thing',
              name: 'Thing four',
            },
            {
              pk: '5',
              sk: 'thing',
              name: 'Thing five',
            },
            {
              pk: '6',
              sk: 'thing',
              name: 'Thing six',
            },
          ],
        },
        UnprocessedKeys: {},
      },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockDdb.on(BatchGetCommand).callsFake(spy);

    const inputParams = {
      RequestItems: {
        t1: {
          Keys: [
            {
              pk: '4',
              sk: 'thing',
            },
            {
              pk: '5',
              sk: 'thing',
            },
            {
              pk: '6',
              sk: 'thing',
            },
          ],
        },
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
    }).batchGet(inputParams);

    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[0], inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });
    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[0], inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });
    expect(spy).to.have.been.calledWith({
      RequestItems: {
        t1: {
          Keys: [inputParams.RequestItems.t1.Keys[0], inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
        },
      },
    });

    expect(data).to.deep.equal({
      Responses: {
        t1: [
          {
            pk: '4',
            sk: 'thing',
            name: 'Thing four',
          },
          {
            pk: '5',
            sk: 'thing',
            name: 'Thing five',
          },
          {
            pk: '6',
            sk: 'thing',
            name: 'Thing six',
          },
        ],
      },
      UnprocessedKeys: {},
      attempts: [
        {
          Responses: {},
          UnprocessedKeys: {
            t1: {
              Keys: [
                {
                  pk: '4',
                  sk: 'thing',
                },
                {
                  pk: '5',
                  sk: 'thing',
                },
                {
                  pk: '6',
                  sk: 'thing',
                },
              ],
            },
          },
        },
        {
          Responses: {},
          UnprocessedKeys: {
            t1: {
              Keys: [
                {
                  pk: '4',
                  sk: 'thing',
                },
                {
                  pk: '5',
                  sk: 'thing',
                },
                {
                  pk: '6',
                  sk: 'thing',
                },
              ],
            },
          },
        },
        {
          Responses: {
            t1: [
              {
                pk: '4',
                sk: 'thing',
                name: 'Thing four',
              },
              {
                pk: '5',
                sk: 'thing',
                name: 'Thing five',
              },
              {
                pk: '6',
                sk: 'thing',
                name: 'Thing six',
              },
            ],
          },
          UnprocessedKeys: {},
        },
      ],
    });
  });

  it('should throw on max retry', async () => {
    const responses = [
      {
        Responses: {
          t1: [
            {
              pk: '1',
              sk: 'thing',
              name: 'Thing One',
            },
          ],
        },
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '2',
                sk: 'thing',
              },
              {
                pk: '3',
                sk: 'thing',
              },
            ],
          },
        },
      },
      {
        Responses: {
          t1: [
            {
              pk: '2',
              sk: 'thing',
              name: 'Thing Two',
            },
          ],
        },
        UnprocessedKeys: {
          t1: {
            Keys: [
              {
                pk: '3',
                sk: 'thing',
              },
            ],
          },
        },
      },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockDdb.on(BatchGetCommand).callsFake(spy);

    const inputParams = {
      RequestItems: {
        t1: {
          Keys: [
            {
              pk: '1',
              sk: 'thing',
            },
            {
              pk: '2',
              sk: 'thing',
            },
            {
              pk: '3',
              sk: 'thing',
            },
          ],
        },
      },
    };

    await new Connector({
      debug: debug('dynamodb'),
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).batchGet(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          RequestItems: {
            t1: {
              Keys: [inputParams.RequestItems.t1.Keys[0], inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
            },
          },
        });
        expect(spy).to.have.been.calledWith({
          RequestItems: {
            t1: {
              Keys: [inputParams.RequestItems.t1.Keys[1], inputParams.RequestItems.t1.Keys[2]],
            },
          },
        });
        expect(spy).to.not.have.been.calledWith({
          RequestItems: {
            t1: {
              Keys: [inputParams.RequestItems.t1.Keys[2]],
            },
          },
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });

  it('should query', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((params) => ({
      Items: [{
        pk: params.ExclusiveStartKey === undefined ? '1' : '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
      LastEvaluatedKey: params.ExclusiveStartKey === undefined ? '1' : undefined,
    }));

    mockDdb.on(QueryCommand).callsFake(spy);

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

  it('should query page', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((params) => ({
      Items: [{
        pk: params.ExclusiveStartKey === undefined ? '1' : '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
      LastEvaluatedKey: params.ExclusiveStartKey === undefined ? '1' : undefined,
    }));
    mockDdb.on(QueryCommand).callsFake(spy);

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
      .queryPage(QUERY_REQUEST);

    expect(spy).to.have.been.calledWith({
      TableName: 'my-service-entities',
      IndexName: 'DataIndex',
      KeyConditionExpression: '#data = :data',
      ExpressionAttributeNames: { '#data': 'data' },
      ExpressionAttributeValues: { ':data': '11' },
      ConsistentRead: true,
    });
    expect(data).to.deep.equal({
      Items: [
        {
          pk: '1',
          sk: 'EVENT',
          data: correlationKey,
          event: {},
        },
      ],
      LastEvaluatedKey: '1',
    });
  });

  it('should scan', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((_) => ({
      Items: [{
        pk: '1',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
    }));
    mockDdb.on(ScanCommand).callsFake(spy);

    const SCAN_REQUEST = {
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': correlationKey,
      },
    };

    const data = await new Connector({
      debug: debug('dynamodb'),
      tableName: 'my-service-entities',
    })
      .scan(SCAN_REQUEST);

    expect(spy).to.have.been.calledWith({
      TableName: 'my-service-entities',
      ExpressionAttributeNames: { '#data': 'data' },
      ExpressionAttributeValues: { ':data': '11' },
    });
    expect(data).to.deep.equal({
      Items: [
        {
          pk: '1',
          sk: 'EVENT',
          data: correlationKey,
          event: {},
        },
      ],
    });
  });

  it('should query with limiting Limit param', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((params) => ({
      Items: [{
        pk: params.ExclusiveStartKey === undefined ? '1' : '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
      LastEvaluatedKey: params.ExclusiveStartKey === undefined ? '1' : undefined,
    }));

    mockDdb.on(QueryCommand).callsFake(spy);

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
      Limit: 1,
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
      Limit: 1,
      ExclusiveStartKey: undefined,
    });
    expect(data).to.deep.equal([
      {
        pk: '1',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      },
    ]);
  });

  it('should query with non-limiting Limit param', async () => {
    const correlationKey = '11';

    const spy = sinon.spy((params) => ({
      Items: [{
        pk: params.ExclusiveStartKey === undefined ? '1' : '2',
        sk: 'EVENT',
        data: correlationKey,
        event: {},
      }],
      LastEvaluatedKey: params.ExclusiveStartKey === undefined ? '1' : undefined,
    }));

    mockDdb.on(QueryCommand).callsFake(spy);

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
      Limit: 10,
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
      Limit: 10,
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
