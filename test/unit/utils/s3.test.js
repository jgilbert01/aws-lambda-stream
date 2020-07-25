import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { putObjectToS3 } from '../../../src/utils/s3';

import Connector from '../../../src/connectors/s3';

describe('utils/s3.js', () => {
  afterEach(sinon.restore);

  it('should put object', (done) => {
    sinon.stub(Connector.prototype, 'putObject').resolves({});

    const uows = [{
      putRequest: {
        Body: JSON.stringify({ f1: 'v1' }),
        Key: 'k1',
      },
    }];

    _(uows)
      .through(putObjectToS3())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          putRequest: {
            Body: JSON.stringify({ f1: 'v1' }),
            Key: 'k1',
          },
          putResponse: {},
        });
      })
      .done(done);
  });
});
