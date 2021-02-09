import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
  envTags,
} from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import { defaultOptions } from '../../../src/utils/opt';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { cdc } from '../../../src/flavors/cdc';
import { skipTag } from '../../../src/filters';

describe('flavors/cdc.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves([]);

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

    initialize({
      ...initializeFrom(rules),
    }, defaultOptions)
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('cdc1');
        expect(collected[0].event.type).to.equal('thing-created');
        expect(collected[0].event.thing).to.deep.equal({
          id: '1',
          name: 'Thing One',
          description: 'This is thing one',
        });
        expect(collected[0].event.tags).to.deep.equal({
          region: 'us-west-2',
          field1: 'v1',
          ...envTags('cdc1'),
          ...skipTag(),
        });
        expect(collected[0].queryRequest).to.be.undefined;
        expect(collected[1].queryRequest).to.not.be.undefined;
        expect(collected[1].queryResponse).to.not.be.undefined;
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
    id: 'cdc1',
    flavor: cdc,
    eventType: /thing-*/,
    filters: [() => true],
    toEvent,
  },
  {
    id: 'cdc2',
    flavor: cdc,
    eventType: /other-*/,
    queryRelated: true,
  },
  {
    id: 'cdc-other1',
    flavor: cdc,
    eventType: 'x9',
  },
];
