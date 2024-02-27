import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';
import { updateExpression, timestampCondition } from '../../../src/sinks/dynamodb';

import {
  initialize, initializeFrom,
} from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { job } from '../../../src/flavors/job';

describe('flavors/job.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
  });

  afterEach(sinon.restore);

  it('should start', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'queryPage')
      .onCall(0)
      .resolves({
        LastEvaluatedKey: {
          pk: '1',
          sk: 'thing',
        },
        Items: [
          {
            pk: '1',
            sk: 'thing',
            name: 'thing 1',
          },
        ],
      })
      .onCall(1)
      .resolves({
        LastEvaluatedKey: {
          pk: '2',
          sk: 'thing',
        },
        Items: [
          {
            pk: '2',
            sk: 'thing',
            name: 'thing 2',
          },
        ],
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
    ]);

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(3);
        expect(collected[0].pipeline).to.equal('job1-started');
        expect(collected[0].querySplitRequest).to.be.deep.equal({
          ExclusiveStartKey: undefined,
          ExpressionAttributeNames: {
            '#discriminator': 'discriminator',
          },
          ExpressionAttributeValues: {
            ':discriminator': 'thing',
          },
          Limit: 2,
        });
        expect(collected[0].emit).to.deep.equal({
          type: 'xyz',
          raw: {
            pk: '1',
            sk: 'thing',
            name: 'thing 1',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'job1-started',
            skip: true,
          },
        });
        expect(collected[1].emit).to.deep.equal({
          type: 'xyz',
          raw: {
            pk: '2',
            sk: 'thing',
            name: 'thing 2',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'job1-started',
            skip: true,
          },
        });
        expect(collected[2].cursorUpdateRequest).to.deep.equal({
          Key: {
            pk: '1',
            sk: 'job',
          },
          ExpressionAttributeNames: {
            '#cursor': 'cursor',
            '#timestamp': 'timestamp',
          },
          ExpressionAttributeValues: {
            ':cursor': {
              pk: '2',
              sk: 'thing',
            },
            ':timestamp': 1572832690000,
          },
          UpdateExpression: 'SET #cursor = :cursor, #timestamp = :timestamp',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
      })
      .done(done);
  });

  it('should continue', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'queryPage')
      .onCall(0)
      .resolves({
        Items: [
          {
            pk: '3',
            sk: 'thing',
            name: 'thing 3',
          },
          {
            pk: '4',
            sk: 'thing',
            name: 'thing 4',
            deleted: true,
          },
        ],
      });

    const events = toDynamodbRecords([
      {
        timestamp: 1572832694,
        keys: {
          pk: '1',
          sk: 'job',
        },
        newImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
          cursor: {
            pk: '2',
            sk: 'thing',
          },
        },
        oldImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
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
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('job1-continued');
        expect(collected[0].querySplitRequest).to.be.deep.equal({
          ExclusiveStartKey: {
            pk: '2',
            sk: 'thing',
          },
          ExpressionAttributeNames: {
            '#discriminator': 'discriminator',
          },
          ExpressionAttributeValues: {
            ':discriminator': 'thing',
          },
          Limit: 2,
        });
        expect(collected[0].querySplitResponse).to.be.deep.equal({
          LastEvaluatedKey: undefined,
          Item: {
            pk: '3',
            sk: 'thing',
            name: 'thing 3',
          },
        });
        expect(collected[0].emit).to.deep.equal({
          type: 'xyz',
          raw: {
            pk: '3',
            sk: 'thing',
            name: 'thing 3',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'job1-continued',
            skip: true,
          },
        });
        expect(collected[1].cursorUpdateRequest).to.deep.equal({
          Key: {
            pk: '1',
            sk: 'job',
          },
          ExpressionAttributeNames: {
            '#cursor': 'cursor',
            '#timestamp': 'timestamp',
          },
          ExpressionAttributeValues: {
            ':timestamp': 1572832694000,
          },
          UpdateExpression: 'SET #timestamp = :timestamp REMOVE #cursor',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
      })
      .done(done);
  });

  it('should stop', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832695,
        keys: {
          pk: '1',
          sk: 'job',
        },
        newImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
        },
        oldImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
          cursor: {
            pk: '2',
            sk: 'thing',
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
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });
});

const toQuerySplitRequest = (uow) => ({
  ExclusiveStartKey: uow.event?.raw?.new?.cursor,
  ExpressionAttributeNames: {
    '#discriminator': 'discriminator',
  },
  ExpressionAttributeValues: {
    ':discriminator': 'thing',
  },
  Limit: 2,
});

const toEvent = (uow) => ({
  type: 'xyz',
  raw: uow.querySplitResponse.Item,
});

const toCursorUpdateRequest = (uow) => ({
  Key: {
    pk: uow.event?.raw?.new?.pk,
    sk: uow.event?.raw?.new?.sk,
  },
  ...updateExpression({
    cursor: uow.querySplitResponse.LastEvaluatedKey || null,
    timestamp: uow.event.timestamp,
  }),
  ...timestampCondition(),
});

const dataFilters = [(uow) => !uow.querySplitResponse?.Item?.deleted];

const rules = [
  {
    id: 'job1-started',
    eventType: 'job-created',
    filters: dataFilters,
    flavor: job,
    toQuerySplitRequest,
    toEvent,
    toCursorUpdateRequest,
  },
  {
    id: 'job1-continued',
    eventType: 'job-updated',
    jobFilters: [(uow) => uow.event?.raw?.new?.cursor],
    filters: dataFilters,
    flavor: job,
    toQuerySplitRequest,
    toEvent,
    toCursorUpdateRequest,
  },
];
