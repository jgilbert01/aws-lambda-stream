import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { claimcheck } from '../../../src/utils/claimcheck';

import Connector from '../../../src/connectors/s3';

describe('utils/claimcheck.js', () => {
  afterEach(sinon.restore);

  it('should get event', (done) => {
    const Body = JSON.stringify({
      id: 'large1',
      type: 'large',
      tags: { source: 's1' },
      thing: {
        id: 't1',
      },
    });

    const stub = sinon.stub(Connector.prototype, 'getObject').resolves({ Body });

    const uows = [{
      event: {
        id: 'large1',
        type: 'large',
        tags: { source: 's1' },
        s3: {
          bucket: 'b1',
          key: 'k1',
        },
      },
    }];

    _(uows)
      .through(claimcheck())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Bucket: 'b1',
          Key: 'k1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          getClaimCheckRequest: {
            Bucket: 'b1',
            Key: 'k1',
          },
          getClaimCheckResponse: {
            Body,
          },
          event: {
            id: 'large1',
            type: 'large',
            tags: { source: 's1' },
            thing: {
              id: 't1',
            },
          },
        });
      })
      .done(done);
  });
});
