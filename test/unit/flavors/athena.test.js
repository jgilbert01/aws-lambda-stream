import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { KmsConnector, MOCK_GEN_DK_RESPONSE } from 'aws-kms-ee';
import {
  initialize, initializeFrom,
} from '../../../src';
import { defaultOptions } from '../../../src/utils/opt';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';
import DynamoDBConnector from '../../../src/connectors/dynamodb';
import SchedulerConnector from '../../../src/connectors/scheduler';
import AthenaConnector from '../../../src/connectors/athena';

import {
  toScheduleRequest,
  toQueryImmediateUpdateRequest,
  executeQuery,
  checkGuard,
  toLinkedQueryRequest,
  toSucceededUpdateRequest,
  toRetryUpdateRequestOrFault,
  toQueryUpdateRequest,
} from '../../../src/flavors/athena';
import { scheduler } from '../../../src/flavors/scheduler';
import { update } from '../../../src/flavors/update';

const rules = [
  // schedule a query
  {
    id: 'schedule-query',
    flavor: scheduler,
    eventType: /^query-(created|updated)/,
    filters: [
      (uow) => uow.event.raw.new.directive === 'SCHEDULE',
    ],
    toScheduleRequest,
  },
  {
    id: 'query-schedule-expired',
    flavor: update,
    eventType: 'athena-query-schedule-expired',
    toUpdateRequest: toQueryImmediateUpdateRequest,
  },
  // run the query
  {
    id: 'my-thing-run',
    flavor: executeQuery,
    eventType: /^query-(created|updated)/,
    filters: [
      // TODO ???
      (uow) => uow.event.queryId === 'my-thing',
      (uow) => uow.event.directive === 'IMMEDIATE',
      //   (uow) => {
      //     console.log(JSON.stringify({ uow }, null, 2));
      //     return uow.event.raw.new.queryId === 'my-thing';
      //   },
      //   (uow) => uow.event.raw.new.directive === 'IMMEDIATE',
    ],
    toOutputLocation: () => 'test',
    toQueryRequest: () => ({ query: 'select * from thing' }),
  },
  // track the query
  {
    id: 'query-failed',
    flavor: update,
    eventType: 'athena-query-failed',
    toQueryRequest: toLinkedQueryRequest,
    toUpdateRequest: toRetryUpdateRequestOrFault,
  },
  {
    id: 'query-succeeded',
    flavor: update,
    eventType: 'athena-query-succeeded',
    toQueryRequest: toLinkedQueryRequest,
    toUpdateRequest: toSucceededUpdateRequest,
  },
  // trigger the next layer of queries
  {
    id: 'my-next-query',
    flavor: update,
    eventType: 'query-updated',
    filters: [
      (uow) => uow.event.raw.new.queryId === 'my-thing',
      (uow) => uow.event.raw.new.directive === 'SUCCEEDED',
    ],
    toUpdateRequest: toQueryUpdateRequest,
  },
];

describe('flavors/athena.js', () => {
  beforeEach(() => {
    sinon.stub(SchedulerConnector.prototype, 'schedule').resolves({});
    sinon.stub(AthenaConnector.prototype, 'startQueryExecution').resolves({ QueryExecutionId: 'q1' });
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves([
      {
        pk: 'my-thing',
        sk: '1',
        discriminator: 'query',
        timestamp: 1600144863435,
        queryId: 'my-thing',
        data: 'q1',
      },
    ]);
    sinon.stub(KmsConnector.prototype, 'generateDataKey').resolves(MOCK_GEN_DK_RESPONSE);
  });
  afterEach(sinon.restore);

  it('should schedule', (done) => {
    const events = toDynamodbRecords([
      // when scheduled initial query
      // then schedule flavor
      {
        timestamp: 1548967023,
        keys: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
        },
        newImage: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
          timestamp: 1769970847308,
          discriminator: 'query',

          queryId: 'my-thing',
          directive: 'SCHEDULE',
          delay: 15,
        },
      },
      // when schedule expired
      // then update immediate
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionkey': 'my-thing_year_2026_month_03_day_07_hour_12',
            'detail-type': 'athena-query-schedule-expired',
            'id': '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
            'source': 'my-lh-test-jobs-dev',
            'time': '2026-02-02T17:27:58Z',
            'detail': {
              sk: '1',
              pk: 'my-thing_year_2026_month_03_day_07_hour_12',
              type: 'athena-query-schedule-expired',
            },
            'type': 'athena-query-schedule-expired',
            'timestamp': '1770053278000',
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
    //   .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[0].pipeline).to.equal('schedule-query');
        expect(collected[0].event.type).to.equal('query-created');
        expect(collected[0].scheduleRequest).to.deep.equal({
          Name: 'undefined-undefined_my-thing_year_2026_month_03_day_07_hour_12',
          Description: 'undefined-undefined_my-thing_year_2026_month_03_day_07_hour_12',
          ClientToken: '0',
          ScheduleExpression: 'at(2026-02-01T18:49:07)',
          Target: {
            EventBridgeParameters: {
              DetailType: 'athena-query-schedule-expired',
              Source: 'undefined-undefined',
            },
            Input: '{"type":"athena-query-schedule-expired","pk":"my-thing_year_2026_month_03_day_07_hour_12","sk":"1"}',
            Arn: 'bus-arn',
            RoleArn: 'scheduler-arn',
          },
          ActionAfterCompletion: 'DELETE',
          FlexibleTimeWindow: {
            Mode: 'OFF',
          },
          KmsKeyArn: undefined,
        });
        expect(collected[0].scheduleResponse).to.deep.equal({});

        expect(collected[1].pipeline).to.equal('query-schedule-expired');
        expect(collected[1].event.type).to.equal('athena-query-schedule-expired');
        expect(collected[1].updateRequest).to.deep.equal({
          Key: {
            pk: 'my-thing_year_2026_month_03_day_07_hour_12',
            sk: '1',
          },
          ExpressionAttributeNames: {
            '#directive': 'directive',
            '#timestamp': 'timestamp',
            '#awsregion': 'awsregion',
          },
          ExpressionAttributeValues: {
            ':directive': 'IMMEDIATE',
            ':timestamp': '1770053278000',
            ':awsregion': 'us-west-2',
          },
          UpdateExpression: 'SET #directive = :directive, #timestamp = :timestamp, #awsregion = :awsregion',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
        expect(collected[1].updateResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should execute', (done) => {
    const events = toDynamodbRecords([
      // when immediate
      // then exec and update exec id
      {
        timestamp: 1548967023,
        keys: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
        },
        newImage: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
          timestamp: 1769970847308,
          discriminator: 'query',
          queryId: 'my-thing',
          directive: 'IMMEDIATE',
        },
        // oldImage: events[0].newImage,
        oldImage: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
          timestamp: 1769970847308,
          discriminator: 'query',
          queryId: 'my-thing',
          directive: 'SCHEDULE',
        },
      },

      // queued > no op
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionkey': 'my-thing_year_2026_month_03_day_07_hour_12',
            'detail-type': 'Athena Query State Change',
            'source': 'aws.athena',
            'id': '091ec135-eb3c-cbe2-c87f-82d91c1cc230',
            'time': '2026-01-26T01:12:25Z',
            'detail': {
              currentState: 'QUEUED',
              queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
              sequenceNumber: '1',
              statementType: 'DML',
              versionId: '0',
              workgroupName: 'my-lh-test-jobs-dev',
            },
            'type': 'athena-query-queued',
            'timestamp': '1770053278000',
          },
        },
      },
      // running > no op
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionkey': 'my-thing_year_2026_month_03_day_07_hour_12',
            'detail-type': 'Athena Query State Change',
            'source': 'aws.athena',
            'id': '091ec135-eb3c-cbe2-c87f-82d91c1cc230',
            'time': '2026-01-26T01:12:25Z',
            'detail': {
              currentState: 'RUNNING',
              queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
              sequenceNumber: '1',
              statementType: 'DML',
              versionId: '0',
              workgroupName: 'my-lh-test-jobs-dev',
            },
            'type': 'athena-query-running',
            'timestamp': '1770053278000',
          },
        },
      },

      // succeeded > update query record
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionKey': 'q1',
            'detail-type': 'Athena Query State Change',
            'source': 'aws.athena',
            'id': '091ec135-eb3c-cbe2-c87f-82d91c1cc230',
            'time': '2026-01-26T01:12:25Z',
            'detail': {
              currentState: 'SUCCEEDED',
              queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
              sequenceNumber: '1',
              statementType: 'DML',
              versionId: '0',
              workgroupName: 'my-lh-test-jobs-dev',
            },
            'type': 'athena-query-succeeded',
            'timestamp': '1770053278000',
          },
        },
      },
    ]);

    // console.log(JSON.stringify(events, null, 2));

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
    //   .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[0].pipeline).to.equal('my-thing-run');
        expect(collected[0].event.type).to.equal('query-updated');
        expect(collected[0].queryRequest).to.deep.equal({
          query: 'select * from thing',
        });
        expect(collected[0].queryResponse).to.deep.equal({ QueryExecutionId: 'q1' });
        expect(collected[0].updateRequest).to.deep.equal({
          TableName: undefined,
          Key: {
            pk: 'my-thing_year_2026_month_03_day_07_hour_12',
            sk: '1',
          },
          ExpressionAttributeNames: {
            '#data': 'data',
            '#awsregion': 'awsregion',
          },
          ExpressionAttributeValues: {
            ':data': 'q1',
            ':awsregion': 'us-west-2',
          },
          UpdateExpression: 'SET #data = :data, #awsregion = :awsregion',
          ReturnValues: 'ALL_NEW',
        });
        expect(collected[0].updateResponse).to.deep.equal({});

        expect(collected[1].pipeline).to.equal('query-succeeded');
        expect(collected[1].event.type).to.equal('athena-query-succeeded');
        expect(collected[1].queryRequest).to.deep.equal({
          IndexName: 'gsi2',
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: {
            '#pk': 'data',
          },
          ExpressionAttributeValues: {
            ':pk': 'q1',
          },
          ConsistentRead: false,
        });
        expect(collected[1].queryResponse).to.deep.equal([{
          pk: 'my-thing',
          sk: '1',
          discriminator: 'query',
          timestamp: 1600144863435,
          queryId: 'my-thing',
          data: 'q1',
        }]);
        expect(collected[1].updateRequest).to.deep.equal({
          Key: {
            pk: 'my-thing',
            sk: '1',
          },
          ExpressionAttributeNames: {
            '#directive': 'directive',
            '#awsregion': 'awsregion',
          },
          ExpressionAttributeValues: {
            ':directive': 'SUCCEEDED',
            ':awsregion': 'us-west-2',
          },
          UpdateExpression: 'SET #directive = :directive, #awsregion = :awsregion',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
        expect(collected[1].updateResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should execute next', (done) => {
    const events = toDynamodbRecords([
      // succeeded > create next query
      {
        timestamp: 1548967023,
        keys: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
        },
        newImage: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
          timestamp: 1769970847308,
          discriminator: 'query',
          queryId: 'my-thing',
          directive: 'SUCCEEDED',
        },
        oldImage: {
          pk: 'my-thing_year_2026_month_03_day_07_hour_12',
          sk: '1',
          timestamp: 1769970847308,
          discriminator: 'query',
          queryId: 'my-thing',
          directive: 'IMMEDIATE',
        },
      },
    ]);

    // console.log(JSON.stringify(events, null, 2));

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
    //   .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0].pipeline).to.equal('my-next-query');
        expect(collected[0].event.type).to.equal('query-updated');
        expect(collected[0].updateRequest).to.deep.equal({
          Key: {
            pk: 'my-next-query',
            sk: '0',
          },
          ExpressionAttributeNames: {
            '#queryId': 'queryId',
            '#directive': 'directive',
            '#discriminator': 'discriminator',
            '#timestamp': 'timestamp',
            '#ttl': 'ttl',
            '#awsregion': 'awsregion',
          },
          ExpressionAttributeValues: {
            ':queryId': 'my-next-query',
            ':directive': 'IMMEDIATE',
            ':discriminator': 'query',
            ':timestamp': 1769970847308,
            ':ttl': 1772822047,
            ':awsregion': 'us-west-2',
          },
          UpdateExpression: 'SET #queryId = :queryId, #directive = :directive, #discriminator = :discriminator, #timestamp = :timestamp, #ttl = :ttl, #awsregion = :awsregion',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
        expect(collected[0].updateResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should retry or fault', (done) => {
    const events = toDynamodbRecords([
      // failed > retry
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionKey': 'q1',
            'id': 'abcdef00-7234-5678-9abc-def012345677',
            'detail-type': 'Athena Query State Change',
            'source': 'aws.athena',
            'time': '2026-01-26T01:12:26Z',
            'detail': {
              athenaError: {
                errorCategory: 2.0, // Value depends on nature of exception
                errorType: 1306.0, // Type depends on nature of exception
                errorMessage: 'Amazon S3 bucket not found', // Message depends on nature of exception
                retryable: true, // Retryable value depends on nature of exception
              },
              versionId: '0',
              currentState: 'FAILED',
              previousState: 'RUNNING',
              statementType: 'DML',
              queryExecutionId: '01234567-0123-0123-0123-012345678901',
              workgroupName: 'primary',
              sequenceNumber: '3',
            },
            'type': 'athena-query-failed',
            'timestamp': '1770053278000',
          },
        },
      },
      // failed > fault
      {
        timestamp: 1548967023,
        keys: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
        },
        newImage: {
          pk: '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
          sk: 'EVENT',
          timestamp: 1770053278000,
          discriminator: 'EVENT',
          event: {
            'partitionKey': 'q1',
            'id': 'abcdef00-7234-5678-9abc-def012345677',
            'detail-type': 'Athena Query State Change',
            'source': 'aws.athena',
            'time': '2026-01-26T01:12:26Z',
            'detail': {
              athenaError: {
                errorCategory: 2.0, // Value depends on nature of exception
                errorType: 1306.0, // Type depends on nature of exception
                errorMessage: 'Amazon S3 bucket not found', // Message depends on nature of exception
                retryable: false, // Retryable value depends on nature of exception
              },
              versionId: '0',
              currentState: 'FAILED',
              previousState: 'RUNNING',
              statementType: 'DML',
              queryExecutionId: '01234567-0123-0123-0123-012345678901',
              workgroupName: 'primary',
              sequenceNumber: '3',
            },
            'type': 'athena-query-failed',
            'timestamp': '1770053278000',
          },
        },
      },
    ]);

    // console.log(JSON.stringify(events, null, 2));

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events)
        .tap((uow) => console.log(JSON.stringify(uow, null, 2))),
      false)
      .errors((err, push) => {
        if (err.uow) {
          push(null, err);
        } else {
          push(err);
        }
      })
      .collect()
      .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[1].pipeline).to.equal('query-failed');
        expect(collected[1].event.type).to.equal('athena-query-failed');
        expect(collected[1].queryRequest).to.deep.equal({
          IndexName: 'gsi2',
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: {
            '#pk': 'data',
          },
          ExpressionAttributeValues: {
            ':pk': 'q1',
          },
          ConsistentRead: false,
        });
        expect(collected[1].queryResponse).to.deep.equal([{
          pk: 'my-thing',
          sk: '1',
          discriminator: 'query',
          timestamp: 1600144863435,
          queryId: 'my-thing',
          data: 'q1',
        }]);
        expect(collected[1].updateRequest).to.deep.equal({
          Key: {
            pk: 'my-thing',
            sk: '1',
          },
          ExpressionAttributeNames: {
            '#directive': 'directive',
            '#delay': 'delay',
            '#retry': 'retry',
            '#awsregion': 'awsregion',
          },
          ExpressionAttributeValues: {
            ':directive': 'SCHEDULE',
            ':delay': 5,
            ':retry': {
              eventID: 'abcdef00-7234-5678-9abc-def012345677',
              timestamp: '1770053278000',
              pipeline: 'my-thing',
            },
            ':awsregion': 'us-west-2',
          },
          UpdateExpression: 'SET #directive = :directive, #delay = :delay, #retry = :retry, #awsregion = :awsregion',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
        expect(collected[1].updateResponse).to.deep.equal({});
      })
      .done(done);
  });

  it('should checkGuard: disabled', () => {
    expect(checkGuard({})({
      event: {
        disableGuard: true,
      },
    }).guard).to.deep.equal({ proceed: true });
  });

  //   it('should checkGuard: wait', () => {
  //     expect(checkGuard({
  //       guard: ['t1', 't2', 't3'],
  //     })({
  //       correlatedTables: [
  //         { notification: { partition: { table: 't1' } } },
  //       ],
  //     }).guard).to.deep.equal({ proceed: false, delay: undefined });
  //   });

  //   it('should checkGuard: proceed', () => {
  //     expect(checkGuard({
  //       guard: ['t1', 't2', 't3'],
  //     })({
  //       correlatedTables: [
  //         { notification: { partition: { table: 't1' } } },
  //         { notification: { partition: { table: 't2' } } },
  //         { notification: { partition: { table: 't3' } } },
  //       ],
  //     }).guard).to.deep.equal({ proceed: true, delay: undefined });
  //   });

  //   it('should checkGuard: delay', () => {
  //     expect(checkGuard({
  //       guard: ['t1', 't2', 't3'],
  //     })({
  //       correlatedTables: [
  //         { notification: { partition: { table: 't1' } } },
  //         { notification: { partition: { table: 't2' } } },
  //         { notification: { partition: { table: 't3' } } },
  //         { notification: { partition: { table: 't1' } } },
  //       ],
  //     }).guard).to.deep.equal({ proceed: false, delay: 15 });
  //   });

  //   it('should checkGuard: custom delay', () => {
  //     expect(checkGuard({
  //       guard: ['t1', 't2', 't3'],
  //       guardDelay: 25,
  //     })({
  //       correlatedTables: [
  //         { notification: { partition: { table: 't1' } } },
  //         { notification: { partition: { table: 't2' } } },
  //         { notification: { partition: { table: 't3' } } },
  //         { notification: { partition: { table: 't1' } } },
  //       ],
  //     }).guard).to.deep.equal({ proceed: false, delay: 25 });
  //   });

  //   it('should checkGuard: alreadyScheduled', () => {
  //     expect(checkGuard({
  //       guard: ['t1', 't2', 't3'],
  //     })({
  //       correlatedTables: [
  //         {
  //           notification: { partition: { table: 't1' } },
  //           disableGuard: true,
  //           delay: 15,
  //           timestamp: 17018511210,
  //           directive: 'SCHEDULE',
  //         },
  //         { notification: { partition: { table: 't2' } } },
  //         { notification: { partition: { table: 't3' } } },
  //       ],
  //       event: {
  //         timestamp: 17718511210,
  //       },
  //     }).guard).to.deep.equal({ proceed: true, delay: undefined });
  //   });
});
