import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, EventBridgeConnector, DynamoDBConnector } from 'aws-lambda-stream';

import { Handler } from '../../../src/trigger';

describe('trigger/evaluate-rules.js', () => {
  beforeEach(() => {
    sinon.stub(DynamoDBConnector.prototype, 'query').resolves([]);
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });
  afterEach(sinon.restore);

  it('should verify evaluate rule eval1', (done) => {
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
            data: '11',
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
        expect(collected[0].pipeline).to.equal('eval1');
        expect(collected[0].emit).to.deep.equal({
          id: '0.eval1',
          type: 'thing-xyz',
          timestamp: 1548967022000,
          partitionKey: '11',
          thing: {
            id: '11',
            name: 'Thing One',
            description: 'This is thing one',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval1',
            skip: true,
          },
          triggers: [
            {
              id: '1',
              type: 'thing-submitted',
              timestamp: 1548967022000,
            },
          ],
        });
      })
      .done(done);
  });
});
