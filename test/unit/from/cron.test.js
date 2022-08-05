import 'mocha';
import { expect } from 'chai';

import { fromCron } from '../../../src/from';

describe('from/cron.js', () => {
  it('should parse records', (done) => {
    const event = {
      time: '2022-08-05:00:00:00',
    };

    fromCron(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            time: '2022-08-05:00:00:00',
          },
          event: {
            time: '2022-08-05:00:00:00',
          },
        });
      })
      .done(done);
  });
});
