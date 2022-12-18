import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
  ttl,
} from '../../../src';

import { toKinesisRecords, fromKinesis } from '../../../src/from/kinesis';

import Connector from '../../../src/connectors/s3';

import { materializeS3 } from '../../../src/flavors/materializeS3';

describe('flavors/materializeS3.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'putObject').resolves({});
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
      {
        type: 'split',
        timestamp: 1548967022000,
        root: {
          things: [{
            id: '2',
            name: 'Thing One',
            description: 'This is thing one',
          }, {
            id: '3',
            name: 'Thing One',
            description: 'This is thing one',
          }],
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(5);
        expect(collected[0].pipeline).to.equal('mv1');
        expect(collected[0].event.type).to.equal('m1');
        expect(collected[0].putRequest).to.deep.equal({
          Key: '1/thing',
          Body: Buffer.from(JSON.stringify({
            id: '1',
            name: 'Thing One',
            description: 'This is thing one',
            discriminator: 'thing',
            ttl: 1549053422,
            timestamp: 1548967022000,
          })),
        });
        expect(collected[1].putRequest.Key).to.equal('2/thing');
        expect(collected[2].putRequest.Key).to.equal('2/thing');
        expect(collected[3].putRequest.Key).to.equal('3/thing');
        expect(collected[4].putRequest.Key).to.equal('3/thing');
      })
      .done(done);
  });
});

const toPutRequest = (uow) => ({
  Key: `${uow.split?.id || uow.event.thing.id}/thing`,
  Body: Buffer.from(JSON.stringify({
    ...(uow.split || uow.event.thing),
    discriminator: 'thing',
    ttl: ttl(uow.event.timestamp, 1),
    timestamp: uow.event.timestamp,
  })),
});

const rules = [
  {
    id: 'mv1',
    flavor: materializeS3,
    eventType: 'm1',
    filters: [() => true],
    toPutRequest,
  },
  {
    id: 'other1',
    flavor: materializeS3,
    eventType: 'x9',
  },
  {
    id: 'split',
    flavor: materializeS3,
    eventType: 'split',
    splitOn: 'event.root.things',
    toPutRequest,
  },
  {
    id: 'split-custom',
    flavor: materializeS3,
    eventType: 'split',
    splitOn: (uow) => uow.event.root.things.map((t) => ({
      ...uow,
      split: t,
    })),
    toPutRequest,
  },
];
