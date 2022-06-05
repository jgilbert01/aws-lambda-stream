import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { KmsConnector, MOCK_GEN_DK_RESPONSE, MOCK_DECRYPT_DK_RESPONSE } from 'aws-kms-ee';

import { decryptEvent, encryptEvent } from '../../../src/utils/encryption';

import { fromDynamodb, toDynamodbRecords } from '../../../src/from/dynamodb';

import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';

import { prefilterOnEventTypes } from '../../../src/filters';

describe('utils/encryption.js', () => {
  afterEach(sinon.restore);

  it('should encrypt an event', (done) => {
    sinon.stub(KmsConnector.prototype, 'generateDataKey').resolves(MOCK_GEN_DK_RESPONSE);

    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
          description: 'd1',
          status: 's1',
        },
      },
    ]);

    fromDynamodb(events)
      .through(encryptEvent({
        eem: {
          fields: ['name', 'description'],
        },
        masterKeyAlias: 'alias/aws-kms-ee',
        AES: false,
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event).to.deep.equal({
          id: '0',
          type: 'thing-created',
          partitionKey: '1',
          timestamp: 1572832690000,
          tags: {
            region: 'us-west-2',
          },
          raw: {
            new: {
              pk: '1',
              sk: 'thing',
              discriminator: 'thing',
              name: 'bjE=',
              description: 'ZDE=',
              status: 's1',
            },
            old: undefined,
          },
          eem: {
            masterKeyAlias: 'alias/aws-kms-ee',
            dataKeys: {
              'us-west-2': 'AQIDAHg5lIgUTrMBJZSEOmrJ/GqVqgcTMUj+cIw/EBA4XAX5TgG+mTJFoKz0VU0tljNQLcGwAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMWhuPTg3e+GjnWx6rAgEQgDsRc7csVqQxagsvXLPx/hAePtadw2GziixRC/UT3FpYU58/NHC8PvFdJDVQ3LkK8XYGLeowpHlC9CaqgA==',
            },
            fields: [
              'name',
              'description',
            ],
          },
        });
      })
      .done(done);
  });

  it('should deencrypt an event', (done) => {
    sinon.stub(KmsConnector.prototype, 'decryptDataKey').resolves(MOCK_DECRYPT_DK_RESPONSE);

    const events = toKinesisRecords([
      {
        id: '0',
        type: 'thing-created',
        partitionKey: '1',
        timestamp: 1572832690000,
        tags: {
          region: 'us-west-2',
        },
        raw: {
          new: {
            pk: '1',
            sk: 'thing',
            discriminator: 'thing',
            name: 'bjE=',
            description: 'ZDE=',
            status: 's1',
          },
        },
        eem: {
          dataKeys: {
            'us-west-2': 'AQIDAHg5lIgUTrMBJZSEOmrJ/GqVqgcTMUj+cIw/EBA4XAX5TgG+mTJFoKz0VU0tljNQLcGwAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMWhuPTg3e+GjnWx6rAgEQgDsRc7csVqQxagsvXLPx/hAePtadw2GziixRC/UT3FpYU58/NHC8PvFdJDVQ3LkK8XYGLeowpHlC9CaqgA==',
          },
          masterKeyAlias: 'alias/aws-kms-ee',
          fields: [
            'name',
            'description',
          ],
        },
      },
    ]);

    fromKinesis(events)
      .through(decryptEvent({
        prefilter: prefilterOnEventTypes([{ eventType: 'thing-created' }]),
        AES: false,
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event).to.deep.equal({
          id: '0',
          type: 'thing-created',
          partitionKey: '1',
          timestamp: 1572832690000,
          tags: {
            region: 'us-west-2',
          },
          raw: {
            new: {
              pk: '1',
              sk: 'thing',
              discriminator: 'thing',
              name: 'n1',
              description: 'd1',
              status: 's1',
            },
          },
        });
      })
      .done(done);
  });

  it('should cover defaults', (done) => {
    encryptEvent()(_([{}])).done(done);
  });

  it('should cover defaults', (done) => {
    decryptEvent()(_([{ event: { eem: {} } }])).done(done);
  });
});
