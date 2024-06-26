import 'mocha';
import { expect } from 'chai';

import { fromEventBridge, toEventBridgeRecord } from '../../../src/from/eventbridge';

describe('from/eventbridge.js', () => {
  it('should parse records', (done) => {
    const event = toEventBridgeRecord({
      type: 't1',
      partitionKey: '1',
    });

    fromEventBridge(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            'version': '0',
            'id': '0',
            'source': 'test',
            'region': 'us-west-2',
            'detail-type': 't1',
            'detail': {
              type: 't1',
              partitionKey: '1',
            },
          },
          event: {
            id: '0',
            type: 't1',
            partitionKey: '1',
          },
        });
      })
      .done(done);
  });
});
