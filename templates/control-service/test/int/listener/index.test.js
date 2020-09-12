import 'mocha';
import { expect } from 'chai';

import { handle } from '../../../src/listener';

describe('listener/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test listener integration', async () => {
    const res = await handle(EVENT, {});
    expect(res).to.equal('Success');
  });
});

const EVENT = {
  Records: [{
    kinesis: {
      kinesisSchemaVersion: '1.0', partitionKey: '"9dc85aab-de5e-45ac-b105-90d25fa764d1"', sequenceNumber: '49610671311194420218916278992418406327582890665088909314', data: 'eyJpZCI6IjQ3ZTIyMTQwLWYzZGYtMTFlYS1hMjA1LTJkZGEyYjkwYThlMSIsInBhcnRpdGlvbktleSI6IjlkYzg1YWFiLWRlNWUtNDVhYy1iMTA1LTkwZDI1ZmE3NjRkMSIsInRpbWVzdGFtcCI6MTU5OTc5NTA4OTc0OSwidGFncyI6eyJyZWdpb24iOiJ1cy1lYXN0LTEiLCJmdW5jdGlvbm5hbWUiOiJldmVudC10ZXN0LXN0Zy1wdWJsaXNoIn0sInR5cGUiOiJ0aGluZy1zdWJtaXR0ZWQifQ==', approximateArrivalTimestamp: 1599795090.125,
    },
    eventSource: 'aws:kinesis',
    eventVersion: '1.0',
    eventID: 'shardId-000000000000:49610671311194420218916278992418406327582890665088909314',
    eventName: 'aws:kinesis:record',
    invokeIdentityArn: 'arn:aws:iam::026257137139:role/my-service-stg-us-east-1-lambdaRole',
    awsRegion: 'us-east-1',
    eventSourceARN: 'arn:aws:kinesis:us-east-1:026257137139:stream/stg-event-hub-s1',
  }],
};
