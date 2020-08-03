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
        type: 'c1',
        timestamp: 1548967022000,
        partitionKey: '1',
        thing: {
          id: '1',
          name: 'Thing One',
          description: 'This is thing one',
        },
        raw: {},
      },
      {
        type: 'x9',
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('clt1');
        expect(collected[0].event.type).to.equal('c1');
        expect(collected[0].putRequest).to.deep.equal({
          Item: {
            pk: 'shardId-000000000000:0',
            sk: 'EVENT',
            discriminator: 'EVENT',
            data: '1',
            ttl: 1549917422,
            timestamp: 1548967022000,
            sequenceNumber: '0',
            event: {
              id: 'shardId-000000000000:0',
              type: 'c1',
              timestamp: 1548967022000,
              partitionKey: '1',
              thing: {
                id: '1',
                name: 'Thing One',
                description: 'This is thing one',
              },
            },
          },
        });
      })
      .done(done);
  });
});

const rules = [
  {
    id: 'clt1',
    flavor: collect,
    eventType: 'c1',
  },
];
