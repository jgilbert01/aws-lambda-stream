import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { KmsConnector, MOCK_GEN_DK_RESPONSE } from 'aws-kms-ee';

import {
  initialize, initializeFrom,
} from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import { defaultOptions } from '../../../src/utils/opt';
import {
  toPkQueryRequest, toGetRequest,
  updateExpression, timestampCondition,
} from '../../../src/utils/dynamodb';
import { DynamoDBConnector } from '../../../src/connectors';

import { upd as update } from '../../../src/flavors/update';

describe('flavors/update.js', () => {
  beforeEach(() => {
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves([]);
    sinon.stub(DynamoDBConnector.prototype, 'batchGet').resolves({
      Responses: {
        undefined: [{
          pk: '2',
          sk: 'thing',
          discriminator: 'thing',
          name: 'thing2',
        }],
      },
      UnprocessedKeys: {},
    });

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
          name: 'Thing One',
          description: 'This is thing one',
          otherThing: 'thing|2',
          ttl: 1549053422,
          timestamp: 1548967022000,
        },
      },
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'other',
        },
        newImage: {
          pk: '1',
          sk: 'other',
          discriminator: 'other',
          name: 'Other One',
          description: 'This is other one',
          ttl: 1549053422,
          timestamp: 1548967022000,
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('update1');
        expect(collected[0].event.type).to.equal('thing-created');
        expect(collected[0].batchGetRequest).to.deep.equal({
          RequestItems: {
            undefined: {
              Keys: [{
                pk: '2',
                sk: 'thing',
              }],
            },
          },
        });
        expect(collected[0].batchGetResponse).to.deep.equal({
          Responses: {
            undefined: [
              {
                pk: '2',
                sk: 'thing',
                discriminator: 'thing',
                name: 'thing2',
              },
            ],
          },
          UnprocessedKeys: {},
        });
        expect(collected[0].updateRequest).to.deep.equal({
          Key: {
            pk: '1',
            sk: 'thing',
          },
          ExpressionAttributeNames: {
            '#pk': 'pk',
            '#sk': 'sk',
            '#discriminator': 'discriminator',
            '#name': 'name',
            '#description': 'description',
            '#otherThing': 'otherThing',
            '#ttl': 'ttl',
            '#timestamp': 'timestamp',
          },
          ExpressionAttributeValues: {
            ':pk': '1',
            ':sk': 'thing',
            ':discriminator': 'thing',
            ':name': 'Thing One',
            ':description': 'This is thing one',
            ':ttl': 1549053422,
            ':timestamp': 1548967022000,
            ':otherThing': {
              pk: '2',
              sk: 'thing',
              discriminator: 'thing',
              name: 'thing2',
            },
          },
          UpdateExpression: 'SET #pk = :pk, #sk = :sk, #discriminator = :discriminator, #name = :name, #description = :description, #otherThing = :otherThing, #ttl = :ttl, #timestamp = :timestamp',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
        expect(collected[0].updateResponse).to.deep.equal({});
        expect(collected[0].queryRequest).to.be.undefined;
        expect(collected[0].getRequest).to.be.undefined;

        expect(collected[1].pipeline).to.equal('update2');
        expect(collected[1].queryRequest).to.not.be.undefined;
        expect(collected[1].queryResponse).to.not.be.undefined;
      })
      .done(done);
  });
});

const toUpdateRequest = (uow) => ({
  Key: {
    pk: uow.event.raw.new.pk,
    sk: 'thing',
  },
  ...updateExpression({
    ...uow.event.raw.new,
    otherThing: uow.batchGetResponse?.Responses.undefined[0],
  }),
  ...timestampCondition(),
});

const rules = [
  {
    id: 'update1',
    flavor: update,
    eventType: /thing-*/,
    filters: [() => true],
    toGetRequest,
    fks: ['otherThing'],
    toUpdateRequest,
  },
  {
    id: 'update2',
    flavor: update,
    eventType: /other-*/,
    toQueryRequest: toPkQueryRequest,
    toUpdateRequest,
  },
  {
    id: 'update-other1',
    flavor: update,
    eventType: 'x9',
  },
];
