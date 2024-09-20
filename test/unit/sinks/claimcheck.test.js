import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { storeClaimcheck } from '../../../src/sinks/claimcheck';

import Connector from '../../../src/connectors/s3';

describe('sinks/claimcheck.js', () => {
  afterEach(sinon.restore);

  it('should store event', (done) => {
    const uows = [
      {}, // not too large
      { // too large
        putClaimcheckRequest: {
          Bucket: 'b1',
          Key: 'us-west-2/claimchecks/2024/09/21/12/1',
          Body: JSON.stringify({ id: '1' }),
        },
      },
      {}, // not too large
    ];

    const stub = sinon.stub(Connector.prototype, 'putObject').resolves({});

    _(uows)
      .batch() // batch in
      .through(storeClaimcheck({
        claimCheckBucketName: 'event-lake-s3',
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1); // assert batch out

        expect(stub).to.have.been.calledWith({
          Bucket: 'b1',
          Key: 'us-west-2/claimchecks/2024/09/21/12/1',
          Body: JSON.stringify({ id: '1' }),
        });

        expect(collected[0]).to.deep.equal([
          {},
          {
            putClaimcheckRequest: {
              Bucket: 'b1',
              Key: 'us-west-2/claimchecks/2024/09/21/12/1',
              Body: JSON.stringify({ id: '1' }),
            },
            putClaimcheckResponse: {},
          },
          {},
        ]);
      })
      .done(done);
  });
});
