import 'mocha';
import { expect } from 'chai';

import { handle } from '../../../src/trigger';

describe('trigger/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test trigger integration', async () => {
    const res = await handle(EVENT, {});
    expect(res).to.equal('Success');
  });
});

const EVENT = {
  Records: [{
    eventID: 'a24f9cdaec8ead2781353ef13e942f42',
    eventName: 'INSERT',
    eventVersion: '1.1',
    eventSource: 'aws:dynamodb',
    awsRegion: 'us-east-1',
    dynamodb: {
      ApproximateCreationDateTime: 1600485986,
      Keys: { sk: { S: 'thing' }, pk: { S: '00000000-0000-0000-0000-000000000000' } },
      NewImage: {
        deleted: { NULL: true }, lastModifiedBy: { S: 'offlineContext_authorizer_principalId' }, sk: { S: 'thing' }, latched: { NULL: true }, pk: { S: '00000000-0000-0000-0000-000000000000' }, ttl: { N: '1601299440' }, timestamp: { N: '1600349040394' },
      },
      SequenceNumber: '7062900000000002294748234',
      SizeBytes: 183,
      StreamViewType: 'NEW_AND_OLD_IMAGES',
    },
    eventSourceARN: 'arn:aws:dynamodb:us-east-1:026257137139:table/stg-my-rest-bff-service-entities/stream/2020-09-17T13:17:40.955',
  }],
};
