import 'mocha';
import { expect } from 'chai';

import { handle } from '../../../src/listener';

describe('listener/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require
  });

  it('should test listener integration', async () => {
    const res = await handle(EVENT, {}, { AES: false });
    expect(res).to.equal('Success');
  });
});

const EVENT = {
  Records: [{
    eventSource: 'aws:kinesis',
    eventVersion: '1.0',
    eventID: 'shardId-000000000000:49610874829222349702089710448058041622161283352021172226',
    eventName: 'aws:kinesis:record',
    awsRegion: 'us-east-1',
    kinesis: {
      sequenceNumber: '0',
      data: 'eyJpZCI6ImEyNGY5Y2RhZWM4ZWFkMjc4MTM1M2VmMTNlOTQyZjQyIiwidHlwZSI6InRoaW5nLWNyZWF0ZWQiLCJwYXJ0aXRpb25LZXkiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDAiLCJ0aW1lc3RhbXAiOjE2MDA0ODU5ODYwMDAsInRhZ3MiOnsiYWNjb3VudCI6ImRldiIsInJlZ2lvbiI6InVzLWVhc3QtMSIsInN0YWdlIjoic3RnIiwic291cmNlIjoiYW5vdGhlci1yZXN0LWJmZi1zZXJ2aWNlIiwiZnVuY3Rpb25uYW1lIjoidW5kZWZpbmVkIiwicGlwZWxpbmUiOiJ0MSIsInNraXAiOmZhbHNlfSwidGhpbmciOnsibGFzdE1vZGlmaWVkQnkiOiJvZmZsaW5lQ29udGV4dF9hdXRob3JpemVyX3ByaW5jaXBhbElkIiwidGltZXN0YW1wIjoxNjAwMzQ5MDQwMzk0LCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCJ9fQ==',
    },
  }],
};

// {
//   id: 'a24f9cdaec8ead2781353ef13e942f42',
//   type: 'thing-created',
//   partitionKey: '00000000-0000-0000-0000-000000000000',
//   timestamp: 1600485986000,
//   tags: {
//     account: 'dev',
//     region: 'us-east-1',
//     stage: 'stg',
//     source: 'another-rest-bff-service',
//     functionname: 'undefined',
//     pipeline: 't1',
//     skip: false
//   },
//   thing: {
//     lastModifiedBy: 'offlineContext_authorizer_principalId',
//     timestamp: 1600349040394,
//     id: '00000000-0000-0000-0000-000000000000'
//   }
// }
