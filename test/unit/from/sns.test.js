import 'mocha';
import { expect } from 'chai';

import { fromSns, toSnsRecords } from '../../../src/from/sns';

describe('from/sns.js', () => {
  it('should parse records', (done) => {
    const event = toSnsRecords([
      {
        msg: 'this is a test',
        timestamp: '1595616620000',
        subject: 's',
      },
    ]);

    fromSns(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            EventSource: 'aws:sns',
            Sns: {
              MessageId: '00000000-0000-0000-0000-000000000000',
              Message: 'this is a test',
              MessageAttributes: {
              },
              Subject: 's',
            },
          },
        });
      })
      .done(done);
  });
});
