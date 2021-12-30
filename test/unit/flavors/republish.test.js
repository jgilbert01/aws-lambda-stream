import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize,
  initializeFrom,
} from '../../../src';

import { toKinesisRecords, fromKinesis } from '../../../src/from/kinesis';

import { defaultOptions } from '../../../src/utils/opt';
import { EventBridgeConnector } from '../../../src/connectors';

import { republish } from '../../../src/flavors/republish';

describe('flavors/republish.js', () => {
  it('should execute', (done) => {
    const stub = sinon
      .stub(EventBridgeConnector.prototype, 'putEvents')
      .resolves({ FailedEntryCount: 0 });

    const rules = [
      {
        id: 'internal-external',
        flavor: republish,
        eventType: 'thing-updated',
        filters: [() => true],
        toEvent: () => ({
          type: 'thing-updated-external',
        }),
        source: 'external',
      },
      {
        id: 'external-internal-simple',
        flavor: republish,
        eventType: 'thing-updated-external',
        toEvent: 'thing-updated',
      },
      {
        id: 'republish-minimal',
        flavor: republish,
        eventType: 'thing2-updated',
      },
      {
        id: 'republish-other',
        flavor: republish,
        eventType: 'x9',
      },
    ];

    const events = toKinesisRecords([
      {
        type: 'thing-updated',
        thing: {
          id: 'thing0',
          name: 'Thing0',
        },
      },
      {
        type: 'thing-updated-external',
        thing: {
          id: 'thing0',
          name: 'Thing0',
        },
      },
      {
        type: 'thing2-updated',
        thing: {
          id: 'thing2',
          name: 'Thing2',
        },
      },
    ]);

    initialize({ ...initializeFrom(rules) }, defaultOptions)
      .assemble(fromKinesis(events), false)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(3);

        expect(collected[2].event).to.deep.equal({
          id: 'shardId-000000000000:0',
          type: 'thing-updated-external',
          thing: {
            id: 'thing0',
            name: 'Thing0',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'internal-external',
            skip: true,
          },
        });
        expect(stub.getCall(2).args[0].Entries[0].Source).to.equal('external');

        expect(collected[0].event).to.deep.equal({
          id: 'shardId-000000000000:1',
          type: 'thing-updated',
          thing: {
            id: 'thing0',
            name: 'Thing0',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'external-internal-simple',
            skip: true,
          },
        });
        expect(stub.getCall(0).args[0].Entries[0].Source).to.equal('custom');

        expect(collected[1].event).to.deep.equal({
          id: 'shardId-000000000000:2',
          type: 'thing2-updated',
          thing: {
            id: 'thing2',
            name: 'Thing2',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'republish-minimal',
            skip: true,
          },
        });
        expect(stub.getCall(1).args[0].Entries[0].Source).to.equal('custom');
      })
      .done(done);
  });
});
