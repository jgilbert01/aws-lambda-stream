import 'mocha';
import { expect } from 'chai';

import { fromAthena } from '../../../src/from/athena';

describe('from/athena.js', () => {
  it('should parse records', (done) => {
    fromAthena(EB_EVENT)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(5);
        expect(collected[0].event.type).to.equal('athena-query-queued');
        expect(collected[1].event.type).to.equal('athena-query-running');
        expect(collected[2].event.type).to.equal('athena-query-succeeded');
        expect(collected[3].event.type).to.equal('athena-query-failed');
        expect(collected[4].event.type).to.equal('athena-query-schedule-expired');
      })
      .done(done);
  });
});

export const EB_EVENT = {
  Records: [
    { // QUEUED
      messageId: 'c9ebf5e9-db02-4afe-b564-971b911e9239',
      body: JSON.stringify({
        'version': '0',
        'id': '091ec135-eb3c-cbe2-c87f-82d91c1cc230',
        'detail-type': 'Athena Query State Change',
        'source': 'aws.athena',
        'account': '012345678912',
        'time': '2026-01-26T01:12:25Z',
        'region': 'us-west-2',
        'resources': [],
        'detail': {
          currentState: 'QUEUED',
          queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
          sequenceNumber: '1',
          statementType: 'DML',
          versionId: '0',
          workgroupName: 'my-lh-test-jobs-dev',
        },
      }),
      attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1769389945297',
        SenderId: 'AIDAKLMT2IB5VGAZG7DSY',
        ApproximateFirstReceiveTimestamp: '1769389945306',
      },
      messageAttributes: {},
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws-us-gov:sqs:us-west-2:012345678912:my-lh-test-jobs-dev-listener-eb',
      awsRegion: 'us-west-2',
    },
    { // RUNNING
      messageId: 'dbf9ed72-fa26-4061-9b46-bd55cbd2e0de',
      body: JSON.stringify({
        'version': '0',
        'id': '5e51fecf-a36b-237c-ce7a-7fb21dd6c4ac',
        'detail-type': 'Athena Query State Change',
        'source': 'aws.athena',
        'account': '012345678912',
        'time': '2026-01-26T01:12:26Z',
        'region': 'us-west-2',
        'resources': [],
        'detail': {
          currentState: 'RUNNING',
          previousState: 'QUEUED',
          queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
          sequenceNumber: '2',
          statementType: 'DML',
          versionId: '0',
          workgroupName: 'my-lh-test-jobs-dev',
        },
      }),
      attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1769389946356',
        SenderId: 'AIDAKLMT2IB5VGAZG7DSY',
        ApproximateFirstReceiveTimestamp: '1769389946357',
      },
      messageAttributes: {},
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws-us-gov:sqs:us-west-2:012345678912:my-lh-test-jobs-dev-listener-eb',
      awsRegion: 'us-west-2',
    },
    { // SUCCEEDED
      messageId: '8b9330ae-ccb2-40c6-a5f3-fc3338a3a9d9',
      body: JSON.stringify({
        'version': '0',
        'id': '8bce653a-9d74-7d7a-de8b-7b80db178467',
        'detail-type': 'Athena Query State Change',
        'source': 'aws.athena',
        'account': '012345678912',
        'time': '2026-01-26T01:12:35Z',
        'region': 'us-west-2',
        'resources': [],
        'detail': {
          currentState: 'SUCCEEDED',
          previousState: 'RUNNING',
          queryExecutionId: 'd3e31b4c-c3e9-4dbd-9cb2-38d0f33ca0f8',
          sequenceNumber: '3',
          statementType: 'DML',
          versionId: '0',
          workgroupName: 'my-lh-test-jobs-dev',
        },
      }),
      attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1769389955415',
        SenderId: 'AIDAKLMT2IB5VGAZG7DSY',
        ApproximateFirstReceiveTimestamp: '1769389955417',
      },
      messageAttributes: {},
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws-us-gov:sqs:us-west-2:012345678912:my-lh-test-jobs-dev-listener-eb',
      awsRegion: 'us-west-2',
    },
    { // FAILED
      messageId: '8b9330ae-ccb7-40c6-a5f3-fc3338a3a9d7',
      body: JSON.stringify({
        'version': '0',
        'id': 'abcdef00-7234-5678-9abc-def012345677',
        'detail-type': 'Athena Query State Change',
        'source': 'aws.athena',
        'account': '012345678912',
        'time': '2026-01-26T01:12:26Z',
        'region': 'us-west-2',
        'resources': [
        ],
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
      }),
      attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1769389955415',
        SenderId: 'AIDAKLMT2IB5VGAZG7DSY',
        ApproximateFirstReceiveTimestamp: '1769389955417',
      },
      messageAttributes: {},
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws-us-gov:sqs:us-west-2:012345678912:my-lh-test-jobs-dev-listener-eb',
      awsRegion: 'us-west-2',
    },
    { // schedule expired
      messageId: 'd1c37cb3-0afa-43ac-83d7-1c045fa013c3',
      body: JSON.stringify({
        'version': '0',
        'id': '6e3e9f4d-8029-87d0-0e07-f3a4e0712225',
        'detail-type': 'athena-query-schedule-expired',
        'source': 'my-lh-test-jobs-dev',
        'account': '012345678912',
        'time': '2026-02-02T17:27:58Z',
        'region': 'us-west-2',
        'resources': [],
        'detail': { type: 'athena-query-schedule-expired', pk: 'nfc-exec-wf-age-group-hsi-report', sk: '1' },
      }),
      attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1770053278876',
        SenderId: 'AIDAKLMT2IB5VGAZG7DSY',
        ApproximateFirstReceiveTimestamp: '1770053278879',
      },
      messageAttributes: {},
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws-us-gov:sqs:us-west-2:012345678912:my-lh-test-jobs-dev-listener-eb',
      awsRegion: 'us-west-2',
    },
  ],
};
