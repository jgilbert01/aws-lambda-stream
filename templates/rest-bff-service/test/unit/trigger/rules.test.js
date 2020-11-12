import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, EventBridgeConnector } from 'aws-lambda-stream';

import { Handler } from '../../../src/trigger';

describe('trigger/rules.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });
  afterEach(sinon.restore);

  it('should verify cdc rule cdc1', (done) => {
    new Handler()
      .handle(toDynamodbRecords([
        {
          timestamp: 1548967023,
          keys: {
            pk: '1',
            sk: 'Thing',
          },
          newImage: {
            pk: '00000000-0000-0000-0000-000000000000',
            sk: 'Thing',
            discriminator: 'Thing',
            ttl: 1551818222,
            name: 'thing0',
          },
        },
      ]), false)
      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('t1');
        expect(collected[0].event).to.deep.equal({
          id: '0',
          type: 'thing-created',
          partitionKey: '1',
          timestamp: 1548967023000,
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 't1',
            skip: true,
          },
          thing: {
            id: '00000000-0000-0000-0000-000000000000',
            name: 'thing0',
          },
          raw: undefined, // removing data from the default event
          // raw: {
          //   new: {
          //     pk: '00000000-0000-0000-0000-000000000000',
          //     sk: 'Thing',
          //     discriminator: 'Thing',
          //     name: 'thing0',
          //     ttl: 1551818222,
          //   },
          //   old: undefined,
          // },
        });
      })
      .done(done);
  });
});
