import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize, initializeFrom } from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import { defaultOptions } from '../../../src/utils/opt';
import { DynamoDBConnector, SqsConnector, SnsConnector } from '../../../src/connectors';

import { sendMessages } from '../../../src/flavors/sendMessages';

describe('flavors/sendMessages.js', () => {
  beforeEach(() => {
    sinon.stub(SqsConnector.prototype, 'sendMessageBatch').resolves({ Failed: [] });
    sinon.stub(SnsConnector.prototype, 'publish').resolves({});
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
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('send1');
        expect(collected[0].event.type).to.equal('thing-created');
        expect(collected[0].message).to.deep.equal({
          MessageBody: JSON.stringify({
            id: '1',
            name: 'Thing One',
            description: 'This is thing one',
          }),
        });
        expect(collected[0].queryRequest).to.be.undefined;
        expect(collected[0].sendMessageBatchResponse).to.deep.equal({ Failed: [] });

        expect(collected[1].pipeline).to.equal('send2');
        expect(collected[1].event.type).to.equal('thing-created');
        expect(collected[1].message).to.deep.equal({
          Message: JSON.stringify({
            id: '1',
            name: 'Thing One',
            description: 'This is thing one',
          }),
        });
        expect(collected[1].publishResponse).to.deep.equal({});
      })
      .done(done);
  });
});

const toSqsMessage = (uow) => ({
  MessageBody: JSON.stringify({
    id: uow.event.raw.new.pk,
    name: uow.event.raw.new.name,
    description: uow.event.raw.new.description,
  }),
});

const toSnsMessage = (uow) => ({
  Message: JSON.stringify({
    id: uow.event.raw.new.pk,
    name: uow.event.raw.new.name,
    description: uow.event.raw.new.description,
  }),
});

const rules = [
  {
    id: 'send1',
    flavor: sendMessages,
    eventType: /thing-*/,
    filters: [() => true],
    toMessage: toSqsMessage,
  },
  {
    id: 'send2',
    flavor: sendMessages,
    eventType: /thing-*/,
    topicArn: 't1',
    toMessage: toSnsMessage,
  },
  {
    id: 'send-other1',
    flavor: sendMessages,
    eventType: 'x9',
  },
];
