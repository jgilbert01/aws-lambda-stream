import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  BatchGetCommand,
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';
import { mockClient } from 'aws-sdk-client-mock';

// import { fromDynamodb, toDynamodbRecords } from '../../../src/from/dynamodb';
import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import { updateExpression } from '../../../src/sinks/dynamodb';
import { initialize, initializeFrom } from '../../../src';
import { defaultOptions } from '../../../src/utils/opt';
import { toPromise, mw } from '../../../src/utils/handler';
import { cdc } from '../../../src/flavors/cdc';
import { materialize } from '../../../src/flavors/materialize';
import { toGetRequest, toPkQueryRequest } from '../../../src/queries/dynamodb';

import { metrics } from '../../../src/metrics';
import Timer from '../../../src/metrics/timer';

const OPTIONS = {
  ...defaultOptions,
};

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
    compact: true,
  },
  {
    id: 'p9',
    flavor: cdc,
    eventType: 'thing-updated',
  },
];

const handle = (event, context, options) => initialize({
  ...initializeFrom(rules),
}, options)
  .assemble(fromKinesis(event), false)
  .through(toPromise);

describe('metrics/index.js', () => {
  let mockEventBridge;
  let mockDdb;
  let datestub;

  beforeEach(() => {
    process.env.METRICS = 'metrics:*';
    process.env.BATCH_SIZE = '10';
    process.env.DISABLED_PIPELINES = 'p9';

    // using aws-sdk-client-mock so that
    // the capture logic within the connectes gets executed
    mockEventBridge = mockClient(EventBridgeClient);
    mockEventBridge.on(PutEventsCommand).resolves({ FailedEntryCount: 0 });

    mockDdb = mockClient(DynamoDBDocumentClient);
    mockDdb.on(UpdateCommand).resolves({});
    mockDdb.on(QueryCommand).resolves({ Items: [] });
    mockDdb.on(BatchGetCommand).resolves({
      Responses: { undefined: [] }, UnprocessedKeys: {},
    });

    const start = 1719020818001; // publish time = 1719020816.001
    datestub = sinon.stub(Timer, 'now');
    for (let i = 1; i < 100; i++) { // eslint-disable-line no-plusplus
      datestub.onCall(i).returns(start + i * 2);
    }
  });
  afterEach(() => {
    sinon.restore();
    mockDdb.restore();
    mockEventBridge.restore();
    delete process.env.DISABLED_PIPELINES;
    delete process.env.BATCH_SIZE;
    delete process.env.METRICS;
    sinon.assert.callCount(datestub, 28);
  });

  it('should measure pipelines', async () => {
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
    ], 1719020816.01);

    return mw(handle, OPTIONS).use(metrics)(events)
      .tap((themetrics) => {
        // console.log(JSON.stringify(themetrics, null, 2));
        expect(themetrics).to.deep.equal({
          'stream.batch.utilization': 0.6,
          'stream.uow.count': 4,
          'p1|stream.pipeline.utilization': 0.75,
          'p2|stream.pipeline.utilization': 0.25,
          'p1|stream.channel.wait.time': {
            average: 2001,
            min: 1993,
            max: 2009,
            sum: 6003,
            count: 3,
          },
          'p1|save|stream.pipeline.io.wait.time': {
            average: 20,
            min: 14,
            max: 26,
            sum: 60,
            count: 3,
          },
          'p1|save|stream.pipeline.io.time': {
            average: 8,
            min: 8,
            max: 8,
            sum: 24,
            count: 3,
          },
          'p1|stream.pipeline.time': {
            average: 34,
            min: 28,
            max: 40,
            sum: 102,
            count: 3,
          },
          'p2|stream.channel.wait.time': {
            average: 2003,
            min: 2003,
            max: 2003,
            sum: 2003,
            count: 1,
          },
          'p2|query|stream.pipeline.io.wait.time': {
            average: 12,
            min: 12,
            max: 12,
            sum: 12,
            count: 1,
          },
          'p2|query|stream.pipeline.io.time': {
            average: 2,
            min: 2,
            max: 2,
            sum: 2,
            count: 1,
          },
          'p2|get|stream.pipeline.io.wait.time': {
            average: 8,
            min: 8,
            max: 8,
            sum: 8,
            count: 1,
          },
          'p2|get|stream.pipeline.io.time': {
            average: 14,
            min: 14,
            max: 14,
            sum: 14,
            count: 1,
          },
          'p2|publish|stream.pipeline.io.wait.time': {
            average: 2,
            min: 2,
            max: 2,
            sum: 2,
            count: 1,
          },
          'p2|publish|stream.pipeline.io.time': {
            average: 2,
            min: 2,
            max: 2,
            sum: 2,
            count: 1,
          },
          'p2|stream.pipeline.time': {
            average: 42,
            min: 42,
            max: 42,
            sum: 42,
            count: 1,
          },
          'p2|stream.pipeline.compact.count': {
            average: 2,
            min: 2,
            max: 2,
            sum: 2,
            count: 1,
          },
          'p2|publish|stream.pipeline.batchSize.count': {
            average: 1,
            min: 1,
            max: 1,
            sum: 1,
            count: 1,
          },
          'p2|publish|stream.pipeline.eventSize.bytes': {
            average: 365,
            min: 365,
            max: 365,
            sum: 365,
            count: 1,
          },
        });
      });
  });
});
