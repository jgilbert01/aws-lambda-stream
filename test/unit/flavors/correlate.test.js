import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
} from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import Connector from '../../../src/connectors/dynamodb';

import { correlate } from '../../../src/flavors/correlate';

describe('flavors/correlate.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'put').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1548967023,
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        newImage: {
          pk: '1',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '1',
          event: {
            id: '1',
            type: 'c1',
            timestamp: 1548967022000,
            partitionKey: '11',
            thing: {
              id: '11',
              name: 'Thing One',
              description: 'This is thing one',
            },
          },
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '3',
          sk: 'EVENT',
        },
        newImage: {
          pk: '3',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '3',
          event: {
            id: '3',
            type: 'c3',
            timestamp: 1548967022000,
            partitionKey: '33',
            thing: {
              id: '33',
              name: 'Thing Three',
              description: 'This is thing three',
              group: 'A',
              category: 'B',
            },
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(RULES),
    })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(3);

        expect(collected[0].pipeline).to.equal('crl1');
        expect(collected[0].event.type).to.equal('c1');
        expect(collected[0].putRequest).to.deep.equal({
          Item: {
            pk: '11',
            sk: '1',
            discriminator: 'CORREL',
            ttl: 1551818222,
            expire: undefined,
            timestamp: 1548967022000,
            sequenceNumber: '0',
            suffix: undefined,
            ruleId: 'crl1',
            awsregion: 'us-west-2',
            event: {
              id: '1',
              type: 'c1',
              timestamp: 1548967022000,
              partitionKey: '11',
              thing: {
                id: '11',
                name: 'Thing One',
                description: 'This is thing one',
              },
            },
          },
        });

        expect(collected[1].pipeline).to.equal('crl2');
        expect(collected[1].event.type).to.equal('c1');
        expect(collected[1].putRequest.Item.pk).to.equal('11.x');
        expect(collected[1].putRequest.Item.ttl).to.equal(1549917422);

        expect(collected[2].pipeline).to.equal('crl3');
        expect(collected[2].event.type).to.equal('c3');
        expect(collected[2].putRequest.Item.pk).to.equal('A|B');
        expect(collected[2].putRequest.Item.expire).to.equal(true);
      })
      .done(done);
  });
});

const RULES = [
  {
    id: 'crl1',
    flavor: correlate,
    eventType: 'c1',
    correlationKey: 'thing.id',
  },
  {
    id: 'crl2',
    flavor: correlate,
    eventType: 'c1',
    correlationKey: 'thing.id',
    correlationKeySuffix: 'x',
    ttl: 11,
  },
  {
    id: 'crl3',
    flavor: correlate,
    eventType: 'c3',
    correlationKey: (uow) => [uow.event.thing.group, uow.event.thing.category].join('|'),
    expire: true,
  },
];
