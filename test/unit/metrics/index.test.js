import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { fromDynamodb, toDynamodbRecords } from '../../../src/from/dynamodb';
import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import { updateExpression } from '../../../src/sinks/dynamodb';
import { initialize, initializeFrom } from '../../../src';
import { defaultOptions } from '../../../src/utils/opt';
import { cdc } from '../../../src/flavors/cdc';
import { materialize } from '../../../src/flavors/materialize';
import { toGetRequest, toPkQueryRequest } from '../../../src/queries/dynamodb';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { toPromiseWithMetrics } from '../../../src/metrics';

describe('utils/metrics.js', () => {
  beforeEach(() => {
    process.env.BATCH_SIZE = 10;
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves([]);
    sinon.stub(DynamoDBConnector.prototype, 'batchGet').resolves({
      Responses: { undefined: [] }, UnprocessedKeys: {},
    });

    const start = 1719020818001; // publish time = 1719020816.001
    const stub = sinon.stub(Date, 'now');
    for (let i = 1; i < 100; i++) { // eslint-disable-line no-plusplus
      stub.onCall(i).returns(start + i * 2);
    }
  });
  afterEach(sinon.restore);

  it('should measure pipelines', async () => {
    const rules = [
      {
        id: 'p1',
        flavor: materialize,
        eventType: 'thing-created',
        toUpdateRequest: async (uow, rule) => ({
          Key: {
            pk: uow.event.raw.new.id,
            sk: 'thing',
          },
          ...updateExpression({
            ...uow.event.thing,
          }),
        }),
      },
      {
        id: 'p2',
        flavor: cdc,
        eventType: 'thing-updated',
        toQueryRequest: toPkQueryRequest,
        toGetRequest,
        fks: ['fk1'],
      },
    ];

    const events = toKinesisRecords([
      {
        id: '1',
        type: 'thing-created',
        timestamp: 1719020810000,
        raw: {
          new: {
            id: '1',
          },
        },
      },
      {
        id: '2',
        type: 'thing-updated',
        timestamp: 1719020811000,
        raw: {
          new: {
            id: '2',
            fk1: 'thing|99',
          },
        },
      },
      {
        id: '3',
        type: 'thing-created',
        timestamp: 1719020812000,
        raw: {
          new: {
            id: '3',
          },
        },
      },
      {
        id: '4',
        type: 'thing-updated',
        timestamp: 1719020813000,
        raw: {
          new: {
            id: '4',
          },
        },
      },
      {
        id: '5',
        type: 'thing-created',
        timestamp: 1719020814000,
        raw: {
          new: {
            id: '5',
          },
        },
      },
      {
        id: '6',
        type: 'something-happened',
        timestamp: 1719020815000,
        raw: {
          new: {
            id: '6',
          },
        },
      },
    ], 1719020816.001);

    return initialize({
      ...initializeFrom(rules),
    }, defaultOptions)
      .assemble(fromKinesis(events), false)
      .through(toPromiseWithMetrics({}))
      .tap((metrics) => {
        console.log('metrics: ');
        console.log(JSON.stringify(metrics, null, 2));
        // expect(collected.length).to.equal(1);
        // expect(collected[0].updateRequest).to.deep.equal({
        // });
      });
  });
});
