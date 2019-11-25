import 'mocha';
import { expect } from 'chai';

import { fromKinesis } from '../../../src/from/kinesis';

import { toKinesisRecords } from '../../../src/utils';

describe('from/kinesis.js', () => {
  it('should parse records', (done) => {
    const event = toKinesisRecords([
      {
        type: 't1',
        partitionKey: '1',
      },
    ]);

    fromKinesis(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventSource: 'aws:kinesis',
            awsRegion: 'us-west-2',
            eventID: 'shardId-000000000000:0',
            kinesis: {
              sequenceNumber: '0',
              data: 'eyJ0eXBlIjoidDEiLCJwYXJ0aXRpb25LZXkiOiIxIn0=',
            },
          },
          event: {
            id: 'shardId-000000000000:0',
            type: 't1',
            partitionKey: '1',
          },
        });
      })
      .done(done);
  });

  it('test handled json parse error', (done) => {
    fromKinesis({
      Records: [
        {
          eventSource: 'aws:kinesis',
          kinesis: {
            sequenceNumber: '0',
            data: Buffer.from('{bad}').toString('base64'),
          },
        },
      ],
    })
      .tap(expect.fail)
      .errors((err, push) => {
        // console.log(err);
        expect(err.name).to.equal('SyntaxError');
        expect(err.message.substring(0, 18)).to.equal('Unexpected token b');
        expect(err.uow).to.be.not.null;
      })
      // .tap(console.log)
      .done(done);
  });
});
