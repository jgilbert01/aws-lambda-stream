import 'mocha';
import { expect } from 'chai';

import { fromFirehose, toFirehoseRecords } from '../../../src/from/firehose';

describe('from/firehose.js', () => {
  it('should parse records', (done) => {
    const event = toFirehoseRecords([
      {
        id: '1',
        type: 't1',
        partitionKey: '1',
      },
      {
        id: 'x',
        type: 't1',
        partitionKey: '1',
        tags: {
          skip: true,
        },
      },
    ]);

    // console.log(JSON.stringify({ event }, null, 2));

    fromFirehose(event)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            recordId: '0',
            approximateArrivalTimestamp: undefined,
            data: 'eyJpZCI6IjEiLCJ0eXBlIjoidDEiLCJwYXJ0aXRpb25LZXkiOiIxIn0=',
          },
          event: {
            id: '1',
            type: 't1',
            partitionKey: '1',
          },
          recordId: '0',
          result: 'Dropped',
        });
      })
      .done(done);
  });
});
