import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, DynamoDBConnector } from 'aws-lambda-stream';

import { Handler } from '../../../src/trigger';

describe('trigger/correlate-rules.js', () => {
  beforeEach(() => {
    sinon.stub(DynamoDBConnector.prototype, 'put').resolves({});
  });
  afterEach(sinon.restore);

  it.skip('should verify correlate rule crl1', (done) => {
    new Handler()
      .handle(toDynamodbRecords([
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
              type: 'thing-submitted',
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
      ]), false)
      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('crl1');
        expect(collected[0].event.type).to.equal('thing-submitted');
        expect(collected[0].putRequest.Item.pk).to.equal('11');
        expect(collected[0].putRequest.Item.sk).to.equal('1');
      })
      .done(done);
  });
});
