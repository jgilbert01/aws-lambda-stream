import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';
import { updateExpression, timestampCondition } from '../../../src/utils/dynamodb';

import {
  initialize, initializeFrom,
} from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { job } from '../../../src/flavors/job';

describe('flavors/job.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves({ Items: [] });
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'scan').resolves({ Items: [{ pk: '1' }, { pk: '2' }] });
    sinon.stub(DynamoDBConnector.prototype, 'queryPage')
      .onCall(0)
      .resolves({
        LastEvaluatedKey: {
          pk: '1',
          sk: 'EVENT',
        },
        Items: [{
          pk: '1',
          sk: 'EVENT',
          data: '11',
        }],
      })
      .onCall(1)
      .resolves({
        Items: [{
          pk: '2',
          sk: 'EVENT',
          data: '22',
        }],
      });

    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'job',
        },
        newImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
        },
      },
      {
        timestamp: 1572832690,
        keys: {
          pk: '2',
          sk: 'cursor',
        },
        newImage: {
          pk: '2',
          sk: 'cursor',
          discriminator: 'process-job',
          cursor: {
            pk: '1',
            sk: 'job',
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(4);
        expect(collected[1].pipeline).to.equal('job1');
        expect(collected[1].scanRequest).to.be.deep.equal({
          ExclusiveStartKey: undefined,
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
        });
        expect(collected[1].emit).to.deep.equal({
          type: 'xyz',
          raw: {
            pk: '1',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'job1',
            skip: true,
          },
        });
        expect(collected[1].queryRequest).to.deep.equal({
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
        });
        expect(collected[3].pipeline).to.equal('flushCursor');
        expect(collected[3].querySplitRequest).to.deep.equal({
          ExclusiveStartKey: {
            pk: '1',
            sk: 'job',
          },
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
          Limit: 1,
        });
        expect(collected[3].cursorUpdateRequest).to.deep.equal({
          Key: {
            pk: '1',
            sk: 'cursor',
          },
          ExpressionAttributeNames: {
            '#discriminator': 'discriminator',
            '#cursor': 'cursor',
          },
          ExpressionAttributeValues: {
            ':discriminator': 'process-job',
            ':cursor': {
              pk: '1',
              sk: 'EVENT',
            },
          },
          UpdateExpression: 'SET #discriminator = :discriminator, #cursor = :cursor',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
      })
      .done(done);
  });
});

const rules = [
  {
    id: 'job1',
    eventType: 'job-created',
    flavor: job,
    filters: [() => true],
    toQueryRequest: (uow) => ({
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': '11',
      },
    }),
    toScanRequest: (uow) => ({
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': '11',
      },
    }),
    toEvent: (uow) => ({
      type: 'xyz',
      raw: uow.scanResponse.Item,
    }),
  },
  {
    id: 'flushCursor',
    eventType: 'process-job-created',
    flavor: job,
    toQuerySplitRequest: (uow) => ({
      ExclusiveStartKey: uow.event?.raw?.new?.cursor || 'abba',
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': '11',
      },
      Limit: 1,
    }),
    toCursorUpdateRequest: (uow) => ({
      Key: {
        pk: '1',
        sk: 'cursor',
      },
      ...updateExpression({
        discriminator: 'process-job',
        cursor: {
          pk: '1',
          sk: 'EVENT',
        },
      }),
      ...timestampCondition(),
    }),
  },
];
