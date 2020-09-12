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
    eventID: '9dc83f7f895c9ec2e521dc3c40f6f1b5',
    eventName: 'INSERT',
    eventVersion: '1.1',
    eventSource: 'aws:dynamodb',
    awsRegion: 'us-east-1',
    dynamodb: {
      ApproximateCreationDateTime: 1599795092,
      Keys: { sk: { S: 'EVENT' }, pk: { S: '47e22140-f3df-11ea-a205-2dda2b90a8e1' } },
      NewImage: {
        sequenceNumber: { S: '49610671311194420218916278992418406327582890665088909314' },
        data: { S: '9dc85aab-de5e-45ac-b105-90d25fa764d1' },
        sk: { S: 'EVENT' },
        pk: { S: '47e22140-f3df-11ea-a205-2dda2b90a8e1' },
        event: {
          M: {
            partitionKey: { S: '9dc85aab-de5e-45ac-b105-90d25fa764d1' }, id: { S: '47e22140-f3df-11ea-a205-2dda2b90a8e1' }, type: { S: 'thing-submitted' }, timestamp: { N: '1599795089749' }, tags: { M: { functionname: { S: 'event-test-stg-publish' }, region: { S: 'us-east-1' } } },
          },
        },
        ttl: { N: '1602646289' },
        discriminator: { S: 'EVENT' },
        timestamp: { N: '1599795089749' },
      },
      SequenceNumber: '100000000022320485486',
      SizeBytes: 437,
      StreamViewType: 'NEW_AND_OLD_IMAGES',
    },
    eventSourceARN: 'arn:aws:dynamodb:us-east-1:026257137139:table/stg-my-service-events/stream/2020-09-11T02:57:49.647',
  }],
};

