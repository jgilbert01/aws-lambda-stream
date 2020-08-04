import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize,
} from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';
import Connector from '../../../src/connectors/eventbridge';

import { expired } from '../../../src/flavors/expired';

describe('flavors/expired.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
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
        newImage: { // not a REMOVE
          discriminator: 'EVENT',
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        oldImage: {
          discriminator: 'EVENT',
          // no ttl
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        oldImage: {
          discriminator: 'EVENT',
          ttl: 1551818222,
          // no expire
        },
      },
      {
        timestamp: 1548967023, // deleted before ttl was reacted
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        oldImage: {
          discriminator: 'EVENT',
          ttl: 1548997023,
          expire: true,
        },
      },
      {
        timestamp: 1551818229,
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        oldImage: {
          pk: '1',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: true,
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
        timestamp: 1551818229,
        keys: {
          pk: '2',
          sk: 'EVENT',
        },
        oldImage: {
          pk: '2',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: 'some-specified-event-type', // testing this difference
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
        timestamp: 1551818229,
        keys: {
          pk: '3',
          sk: 'EVENT',
        },
        oldImage: {
          pk: '3',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: true,
          data: '1',
          event: {
            id: '1',
            type: 'namespace.entity.action', // testing this difference
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
    ]);

    initialize({
      expired,
    }, defaultOptions)
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(3);

        expect(collected[0].pipeline).to.equal('expired');
        expect(collected[0].event.type).to.equal('event-deleted');
        expect(collected[0].emit).to.deep.equal({
          id: '4',
          type: 'c1-expired',
          timestamp: 1551818222000,
          partitionKey: '11',
          thing: {
            id: '11',
            name: 'Thing One',
            description: 'This is thing one',
          },
          triggers: [
            {
              id: '1',
              type: 'c1',
              timestamp: 1548967022000,
            },
          ],
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'expired',
            skip: true,
          },
        });

        expect(collected[1].emit.type).to.equal('some-specified-event-type');
        expect(collected[2].emit.type).to.equal('namespace.entity.action.expired');
      })
      .done(done);
  });
});
