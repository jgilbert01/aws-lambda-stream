import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize, initializeFrom } from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import { defaultOptions } from '../../../src/utils/opt';
import { WebSocketConnector } from '../../../src/connectors';

import { broadcastToWebSocket } from '../../../src/flavors/websocket';

describe('flavors/websocket.js', () => {
  beforeEach(() => {
    sinon.stub(WebSocketConnector.prototype, 'postToConnection').resolves({});
  });

  afterEach(sinon.restore);

  it('should broadcast to matching connections', (done) => {
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
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[0].pipeline).to.equal('broadcast1');
        expect(collected[0].connectionId).to.equal('conn-1');
        expect(collected[0].message).to.deep.equal({
          type: 'thing-created',
          id: '1',
          name: 'Thing One',
        });
        expect(collected[0].postResponse).to.deep.equal({});

        expect(collected[1].connectionId).to.equal('conn-2');
        expect(collected[1].message).to.deep.equal({
          type: 'thing-created',
          id: '1',
          name: 'Thing One',
        });
      })
      .done(done);
  });

  it('should skip non-matching event types', (done) => {
    const events = toDynamodbRecords([
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
      .tap((collected) => {
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });

  it('should pass uow through when toConnections is not defined', (done) => {
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
          ttl: 1549053422,
          timestamp: 1548967022000,
        },
      },
    ]);

    initialize({
      ...initializeFrom(rulesNoConnections),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].connectionId).to.be.undefined;
        expect(collected[0].postResponse).to.deep.equal({});
      })
      .done(done);
  });
});

const toMessage = (uow) => ({
  type: uow.event.type,
  id: uow.event.raw.new.pk,
  name: uow.event.raw.new.name,
});

const toConnections = () => Promise.resolve([
  { connectionId: 'conn-1' },
  { connectionId: 'conn-2' },
]);

const rules = [
  {
    id: 'broadcast1',
    flavor: broadcastToWebSocket,
    eventType: /thing-.*/,
    toMessage,
    toConnections,
  },
];

const rulesNoConnections = [
  {
    id: 'broadcast2',
    flavor: broadcastToWebSocket,
    eventType: /thing-.*/,
    toMessage,
  },
];
