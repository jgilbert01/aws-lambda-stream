import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { publishToSns } from '../../../src/sinks/sns';

import Connector from '../../../src/connectors/sns';

describe('sinks/sns.js', () => {
  afterEach(sinon.restore);

  it('should publish', (done) => {
    sinon.stub(Connector.prototype, 'publish').resolves({});

    const uows = [{
      message: {
        Message: JSON.stringify({ f1: 'v1' }),
      },
    }];

    _(uows)
      .through(publishToSns())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          message: {
            Message: JSON.stringify({ f1: 'v1' }),
          },
          publishResponse: {},
        });
      })
      .done(done);
  });
});
