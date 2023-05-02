import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { ratelimit } from '../../../src/utils';

describe('utils/ratelimit.js', () => {
  afterEach(sinon.restore);

  it('should ratelimit', (done) => {
    const uows = [
      {
        event: {
          partitionKey: '1',
        },
      },
      {
        event: {
          partitionKey: '1',
        },
      },
      {
        event: {
          partitionKey: '2',
        },
      },
    ];

    _(uows)
      .through(ratelimit({ rate: { num: 2, ms: 100 } }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(3);
        expect(collected).to.deep.equal([
          {
            event: {
              partitionKey: '1',
            },
          },
          {
            event: {
              partitionKey: '1',
            },
          },
          {
            event: {
              partitionKey: '2',
            },
          },
        ]);
      })
      .done(done);
  }).timeout(300);
});
