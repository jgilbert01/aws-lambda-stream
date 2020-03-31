import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, assemble, initializeFrom,
  envTags,
} from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import Connector from '../../../src/connectors/kinesis';

import crud from '../../../src/flavors/crud';
import { skipTag } from '../../../src/filters';

describe('flavors/crud.js', () => {
  beforeEach(() => {
    initialize({
      ...initializeFrom(rules),
    });

    sinon.stub(Connector.prototype, 'putRecords').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
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

    assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('crud1');
        expect(collected[0].event.type).to.equal('thing-created');
        expect(collected[0].event.thing).to.deep.equal({
          id: '1',
          name: 'Thing One',
          description: 'This is thing one',
        });
        expect(collected[0].event.tags).to.deep.equal({
          region: 'us-west-2',
          field1: 'v1',
          ...envTags('crud1'),
          ...skipTag(),
        });
      })
      .done(done);
  });
});

const toEvent = (uow) => ({
  thing: {
    id: uow.event.raw.new.pk,
    name: uow.event.raw.new.name,
    description: uow.event.raw.new.description,
  },
  tags: {
    ...uow.event.tags,
    field1: 'v1',
  },
});

const rules = [
  {
    id: 'crud1',
    flavor: crud,
    eventType: /thing-*/,
    filters: [() => true],
    toEvent,
  },
  {
    id: 'crud2',
    flavor: crud,
    eventType: /other-*/,
  },
  {
    id: 'crud-other1',
    flavor: crud,
    eventType: 'x9',
  },
];
