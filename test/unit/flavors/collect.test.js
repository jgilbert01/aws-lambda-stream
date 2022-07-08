import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
} from '../../../src';

import { toKinesisRecords, fromKinesis } from '../../../src/from/kinesis';

import Connector from '../../../src/connectors/dynamodb';

import { collect } from '../../../src/flavors/collect';

describe('flavors/collect.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'put').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    const events = toKinesisRecords([
      {
        id: '1',
        type: 'c1',
        timestamp: 1548967022000,
        partitionKey: '11',
        thing: {
          id: '11',
          name: 'Thing One',
          description: 'This is thing one',
        },
        raw: {},
      },
      {
        id: '2',
        type: 'c2',
        timestamp: 1548967022000,
        partitionKey: '22',
        thing: {
          id: '22',
          name: 'Thing Two',
          description: 'This is thing two',
          group: 'C',
        },
        raw: {},
      },
      {
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
        raw: {},
      },
      {
        type: 'x9',
      },
    ]);

    initialize({
      ...initializeFrom(RULES),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(3);

        expect(collected[0].pipeline).to.equal('clt1');
        expect(collected[0].event.type).to.equal('c1');
        expect(collected[0].putRequest).to.deep.equal({
          Item: {
            pk: '1',
            sk: 'EVENT',
            discriminator: 'EVENT',
            data: '11',
            ttl: 1551818222,
            expire: undefined,
            timestamp: 1548967022000,
            sequenceNumber: '0',
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

        expect(collected[1].pipeline).to.equal('clt2');
        expect(collected[1].event.type).to.equal('c2');
        expect(collected[1].putRequest.Item.data).to.equal('C');

        expect(collected[2].pipeline).to.equal('clt3');
        expect(collected[2].event.type).to.equal('c3');
        expect(collected[2].putRequest.Item.data).to.equal('A|B');
        expect(collected[2].putRequest.Item.expire).to.equal(true);
      })
      .done(done);
  });
});

const RULES = [
  {
    id: 'clt1',
    flavor: collect,
    eventType: 'c1',
  },
  {
    id: 'clt2',
    flavor: collect,
    eventType: 'c2',
    correlationKey: 'thing.group',
  },
  {
    id: 'clt3',
    flavor: collect,
    eventType: 'c3',
    correlationKey: (uow) => [uow.event.thing.group, uow.event.thing.category].join('|'),
    expire: true,
  },
];
