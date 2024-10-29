import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { storeClaimcheck } from '../../../src/sinks/claimcheck';
import { batchWithSize } from '../../../src/utils/batch';
import * as time from '../../../src/utils/time';

import Connector from '../../../src/connectors/s3';

describe('sinks/claimcheck.js', () => {
  afterEach(sinon.restore);

  it('should handle oversized requests', (done) => {
    sinon.stub(time, 'now').returns(new Date(1726854864001));
    const spy = sinon.spy();
    const uows = [
      {
        publishRequestEntry: {
          Detail: JSON.stringify({ // size = 23
            id: '1',
            body: 'xxx',
          }),
        },
      },
      {
        publishRequestEntry: {
          Detail: JSON.stringify({ // size = 33
            id: '2',
            body: 'xxxxxxxxxxxxx',
          }),
        },
      },
      {
        publishRequestEntry: {
          Detail: JSON.stringify({ // size = 140
            id: '3',
            body: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
          }),
        },
      },
    ];

    _(uows)
      .consume(batchWithSize({
        batchSize: 10,
        maxRequestSize: 100,
        requestEntryField: 'publishRequestEntry',
        claimCheckBucketName: 'event-lake-s3',
      }))
      .errors(spy)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(2);
        expect(spy).to.not.have.been.called;
        expect(collected[1]).to.deep.equal([
          {
            publishRequestEntry: { // size = 39
              id: '3',
              type: undefined,
              partitionKey: undefined,
              timestamp: undefined,
              tags: undefined,
              s3: {
                bucket: 'event-lake-s3',
                key: 'us-west-2/claimchecks/2024/8/20/17/3',
              },
            },
            putClaimcheckRequest: {
              Bucket: 'event-lake-s3',
              Key: 'us-west-2/claimchecks/2024/8/20/17/3',
              Body: '{\"id\":\"3\",\"body\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}',
            },
          },
        ]);
      })
      .done(done);
  });

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
