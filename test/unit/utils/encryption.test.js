import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { KmsConnector, MOCK_GEN_DK_RESPONSE, MOCK_DECRYPT_DK_RESPONSE } from 'aws-kms-ee';

import {
  decryptEvent, encryptEvent, decryptChangeEvent, encryptData, decryptData,
} from '../../../src/utils';
import { fromDynamodb, toDynamodbRecords } from '../../../src/from/dynamodb';
import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import { prefilterOnEventTypes } from '../../../src/filters';
import { updateExpression } from '../../../src/sinks/dynamodb';
import { initialize, initializeFrom } from '../../../src';
import { materialize } from '../../../src/flavors/materialize';
import Connector from '../../../src/connectors/dynamodb';

describe('utils/encryption.js', () => {
  beforeEach(() => {
    sinon.stub(KmsConnector.prototype, 'generateDataKey').resolves(MOCK_GEN_DK_RESPONSE);
    sinon.stub(KmsConnector.prototype, 'decryptDataKey').resolves(MOCK_DECRYPT_DK_RESPONSE);
    sinon.stub(Connector.prototype, 'update').resolves({});
  });
  afterEach(sinon.restore);

  describe('event encryption', () => {
    it('should encrypt an event - trigger function', (done) => {
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
                name: 'Im4xIg==',
                description: 'ImQxIg==',
                status: 's1',
              },
              old: undefined,
            },
            eem: {
              masterKeyAlias: 'alias/aws-kms-ee',
              dataKeys: {
                'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
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

    it('should decrypt an event - listener function', (done) => {
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
              'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
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

  describe('data encryption', () => {
    it('should encrypt data - listener function', (done) => {
      const rule1 = {
        id: 'e1',
        flavor: materialize,
        eventType: 'thing-created',
        toUpdateRequest: async (uow, rule) => ({
          Key: {
            pk: uow.event.thing.id,
            sk: 'thing',
          },
          ...updateExpression(await rule.encrypt({
            ...uow.event.thing,
            discriminator: 'thing',
            timestamp: uow.event.timestamp,
          })),
        }),
        eem: {
          fields: [
            'name',
            'description',
          ],
        },
        masterKeyAlias: 'alias/aws-kms-ee',
        AES: false,
      };

      const events = toKinesisRecords([{
        id: '0',
        type: 'thing-created',
        timestamp: 1572832690000,
        thing: {
          id: '1',
          name: 'n1',
          description: 'd1',
          status: 's1',
        },
      }]);

      initialize({
        ...initializeFrom([rule1]),
      })
        .assemble(fromKinesis(events), false)
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));
          expect(collected.length).to.equal(1);
          expect(collected[0].updateRequest).to.deep.equal({
            Key: {
              pk: '1',
              sk: 'thing',
            },
            ExpressionAttributeNames: {
              '#id': 'id',
              '#name': 'name',
              '#description': 'description',
              '#status': 'status',
              '#discriminator': 'discriminator',
              '#timestamp': 'timestamp',
              '#eem': 'eem',
            },
            ExpressionAttributeValues: {
              ':id': '1',
              ':name': 'Im4xIg==',
              ':description': 'ImQxIg==',
              ':status': 's1',
              ':discriminator': 'thing',
              ':timestamp': 1572832690000,
              ':eem': {
                dataKeys: {
                  'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
                },
                masterKeyAlias: 'alias/aws-kms-ee',
                fields: [
                  'name',
                  'description',
                ],
              },
            },
            UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #status = :status, #discriminator = :discriminator, #timestamp = :timestamp, #eem = :eem',
            ReturnValues: 'ALL_NEW',
          });
        })
        .done(done);
    });

    it('should decrypt data - query function', async () => {
      const encryptedQueryResults = [{
        id: '1',
        name: 'Im4xIg==',
        description: 'ImQxIg==',
        status: 's1',
        eem: {
          masterKeyAlias: 'alias/aws-kms-ee',
          dataKeys: {
            'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
          },
          fields: [
            'name',
            'description',
          ],
        },
      }];

      const opt = {
        eemField: 'eem',
        AES: false,
      };

      // mimicing configured encryptors passed to domain model constructor
      opt.decrypt = decryptData(opt);

      const unencryptedQueryResults = await Promise.all(encryptedQueryResults
        .map(async (item) => opt.decrypt(item)));

      expect(unencryptedQueryResults).to.deep.equal([{
        id: '1',
        name: 'n1',
        description: 'd1',
        status: 's1',
      }]);
    });

    it('should encrypt and decrypt', async () => {
      const rule = {
        eemField: 'eem',
        eem: {
          fields: [
            'name',
            'description',
          ],
        },
        masterKeyAlias: 'alias/aws-kms-ee',
        AES: false,
      };

      const encrypt = encryptData(rule);
      const decrypt = decryptData(rule);

      const encryptResponse = await (encrypt({
        id: '1',
        name: 'n1',
        description: 'd1',
        status: 's1',
      }));

      const decryptResponse = await (decrypt(encryptResponse));

      expect(encryptResponse).to.deep.equal({
        id: '1',
        name: 'Im4xIg==',
        description: 'ImQxIg==',
        status: 's1',
        eem: {
          masterKeyAlias: 'alias/aws-kms-ee',
          dataKeys: {
            'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
          },
          fields: [
            'name',
            'description',
          ],
        },
      });

      expect(decryptResponse).to.deep.equal({
        id: '1',
        name: 'n1',
        description: 'd1',
        status: 's1',
      });
    });

    it('should not decrypt', async () => {
      // cover defaults
      encryptData();
      const decrypt = decryptData();

      // no fields
      expect(await decrypt({})).to.deep.equal({});
      // no eem field
      expect(await decrypt({ f1: 'v1' })).to.deep.equal({ f1: 'v1' });
    });

    it('should decrypt a change event - trigger function - created', (done) => {
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
            name: 'Im4xIg==',
            description: 'ImQxIg==',
            status: 's1',
            eem: {
              masterKeyAlias: 'alias/aws-kms-ee',
              dataKeys: {
                'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
              },
              fields: [
                'name',
                'description',
              ],
            },
          },
        },
      ]);

      fromDynamodb(events)
        .through(decryptChangeEvent({
          prefilter: prefilterOnEventTypes([{ eventType: /^thing-(created|updated|deleted)/ }]),
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
              old: undefined,
            },
          });
        })
        .done(done);
    });

    it('should decrypt a change event - trigger function - updated', (done) => {
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
            name: 'Im4xIg==',
            description: 'ImQxIg==',
            status: 's1',
            eem: {
              masterKeyAlias: 'alias/aws-kms-ee',
              dataKeys: {
                'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
              },
              fields: [
                'name',
                'description',
              ],
            },
          },
          oldImage: {
            pk: '1',
            sk: 'thing',
            discriminator: 'thing',
            name: 'Im4xIg==',
            description: 'ImQxIg==',
            status: 's1',
            eem: {
              masterKeyAlias: 'alias/aws-kms-ee',
              dataKeys: {
                'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
              },
              fields: [
                'name',
                'description',
              ],
            },
          },
        },
      ]);

      fromDynamodb(events)
        .through(decryptChangeEvent({
          prefilter: prefilterOnEventTypes([{ eventType: /^thing-(created|updated|deleted)/ }]),
          AES: false,
        }))
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(1);
          expect(collected[0].event).to.deep.equal({
            id: '0',
            type: 'thing-updated',
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
              old: {
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

    it('should decrypt a change event - trigger function - deleted', (done) => {
      const events = toDynamodbRecords([
        {
          timestamp: 1572832690,
          keys: {
            pk: '1',
            sk: 'thing',
          },
          oldImage: {
            pk: '1',
            sk: 'thing',
            discriminator: 'thing',
            name: 'Im4xIg==',
            description: 'ImQxIg==',
            status: 's1',
            eem: {
              masterKeyAlias: 'alias/aws-kms-ee',
              dataKeys: {
                'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
              },
              fields: [
                'name',
                'description',
              ],
            },
          },
        },
      ]);

      fromDynamodb(events)
        .through(decryptChangeEvent({
          prefilter: prefilterOnEventTypes([{ eventType: /^thing-(created|updated|deleted)/ }]),
          AES: false,
        }))
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(1);
          expect(collected[0].event).to.deep.equal({
            id: '0',
            type: 'thing-deleted',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: undefined,
              old: {
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

    it('should cover skip - new', (done) => {
      decryptChangeEvent()(_([{ event: { raw: { new: {} } } }])).done(done);
    });

    it('should cover skip - old', (done) => {
      decryptChangeEvent()(_([{ event: { raw: { old: {} } } }])).done(done);
    });

    it('should cover skip - prefilter', (done) => {
      decryptChangeEvent()(_([{ event: { raw: { new: { eem: {} } } } }])).done(done);
    });
  });
});
