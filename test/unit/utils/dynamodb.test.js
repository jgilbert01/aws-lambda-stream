import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { ttl } from '../../../src/utils';

import {
  updateExpression, timestampCondition, update, put, query, batchGet,
} from '../../../src/utils/dynamodb';

import Connector from '../../../src/connectors/dynamodb';

describe('utils/dynamodb.js', () => {
  afterEach(sinon.restore);

  it('should calculate ttl', () => {
    expect(ttl(1540454400000, 30)).to.equal(1543046400);
  });

  it('should calculate updateExpression', () => {
    expect(updateExpression({
      id: '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
      name: 'Thing One',
      description: 'This is thing one.',
      status: undefined,
      status2: null,
      discriminator: 'thing',
      latched: true,
      ttl: ttl(1540454400000, 30),
      timestamp: 1540454400000,
    })).to.deep.equal({
      ExpressionAttributeNames: {
        '#description': 'description',
        '#discriminator': 'discriminator',
        '#id': 'id',
        '#latched': 'latched',
        '#name': 'name',
        // '#status': 'status',
        '#status2': 'status2',
        '#timestamp': 'timestamp',
        '#ttl': 'ttl',
      },
      ExpressionAttributeValues: {
        ':description': 'This is thing one.',
        ':discriminator': 'thing',
        ':id': '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
        ':latched': true,
        ':name': 'Thing One',
        // ':status': undefined,
        // ':status2': null,
        ':timestamp': 1540454400000,
        ':ttl': 1543046400,
      },
      UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp REMOVE #status2',
      ReturnValues: 'ALL_NEW',
    });
  });

  it('should calculate timestampCondition', () => {
    expect(timestampCondition()).to.deep.equal({
      ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
    });
  });

  it('should call update', (done) => {
    const stub = sinon.stub(Connector.prototype, 'update').resolves({});

    const uows = [{
      updateRequest: {
        Key: {
          pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
          sk: 'thing',
        },
      },
    }];

    _(uows)
      .through(update())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(stub).to.have.been.calledWith({
          Key: {
            pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
            sk: 'thing',
          },
        });
        expect(collected[0].updateResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should call put', (done) => {
    const stub = sinon.stub(Connector.prototype, 'put').resolves({});

    const uows = [{
      putRequest: {
        Item: {
          pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
          sk: 'thing',
        },
      },
    }];

    _(uows)
      .through(put())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(stub).to.have.been.calledWith({
          Item: {
            pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
            sk: 'thing',
          },
        });
        expect(collected[0].putResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should call batchGet', (done) => {
    const stub = sinon.stub(Connector.prototype, 'batchGet').resolves({
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

    const uows = [{
      batchGetRequest: {
        RequestItems: {
          'stg-my-service-events': {
            Keys: [{
              pk: '1',
              sk: 'thing',
            }],
          },
        },
      },
    }];

    _(uows)
      .through(batchGet())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(stub).to.have.been.calledWith({
          RequestItems: {
            'stg-my-service-events': {
              Keys: [{
                pk: '1',
                sk: 'thing',
              }],
            },
          },
        });
        expect(collected[0].batchGetResponse).to.deep.equal({
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
      })
      .done(done);
  });

  it('should call query', (done) => {
    const stub = sinon.stub(Connector.prototype, 'query').resolves([{
      pk: '1',
      sk: 'EVENT',
      data: '11',
      event: {},
    }]);

    const uows = [
      {
        queryRequest: {
          IndexName: 'DataIndex',
          KeyConditionExpression: '#data = :data',
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
          ConsistentRead: true,
        },
      },
      {
        queryRequest: undefined,
      },
      { // cached
        queryRequest: {
          IndexName: 'DataIndex',
          KeyConditionExpression: '#data = :data',
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
          ConsistentRead: true,
        },
      },
    ];

    _(uows)
      .through(query({ parallel: 1 }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(3);
        expect(stub).to.have.been.calledWith({
          IndexName: 'DataIndex',
          KeyConditionExpression: '#data = :data',
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
          ConsistentRead: true,
        });
        expect(collected[0].queryResponse).to.deep.equal([{
          pk: '1',
          sk: 'EVENT',
          data: '11',
          event: {},
        }]);

        expect(collected[1].queryResponse).to.be.undefined;

        expect(collected[2].queryResponse).to.deep.equal([{
          pk: '1',
          sk: 'EVENT',
          data: '11',
          event: {},
        }]);
      })
      .done(done);
  });
});
