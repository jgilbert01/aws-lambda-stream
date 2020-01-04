import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, execute, initializeFrom,
  toKinesisRecords, fromKinesis,
  ttl, updateExpression, timestampCondition, DynamoDBConnector,
} from '../../../src';

import materialize from '../../../src/flavors/materialize';

describe('flavors/materialize.js', () => {
  beforeEach(() => {
    initialize({
      ...initializeFrom(rules),
    });

    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    const events = toKinesisRecords([
      {
        type: 'm1',
        timestamp: 1548967022000,
        thing: {
          id: '1',
          name: 'Thing One',
          description: 'This is thing one',
        },
      },
    ]);

    execute(fromKinesis(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('mv1');
        expect(collected[0].event.type).to.equal('m1');
        expect(collected[0].updateRequest).to.deep.equal({
          Key: {
            pk: '1',
            sk: 'thing',
          },
          ExpressionAttributeNames: {
            '#id': 'id',
            '#name': 'name',
            '#description': 'description',

            '#discriminator': 'discriminator',
            '#ttl': 'ttl',
            '#timestamp': 'timestamp',
          },
          ExpressionAttributeValues: {
            ':id': '1',
            ':name': 'Thing One',
            ':description': 'This is thing one',
            ':discriminator': 'thing',
            ':ttl': 1549053422,
            ':timestamp': 1548967022000,
          },
          UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #ttl = :ttl, #timestamp = :timestamp',
          ReturnValues: 'ALL_NEW',
          ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
        });
      })
      .done(done);
  });
});

const toUpdateRequest = (uow) => ({
  Key: {
    pk: uow.event.thing.id,
    sk: 'thing',
  },
  ...updateExpression({
    ...uow.event.thing,
    discriminator: 'thing',
    ttl: ttl(uow.event.timestamp, 1),
    timestamp: uow.event.timestamp,
  }),
  ...timestampCondition(),
});

const rules = [
  {
    id: 'mv1',
    pipeline: materialize,
    eventType: 'm1',
    filters: [() => true],
    toUpdateRequest,
  },
  {
    id: 'other1',
    pipeline: materialize,
    eventType: 'x9',
  },
];
