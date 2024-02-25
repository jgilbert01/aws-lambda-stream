import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { fetch } from '../../../src/sinks/fetch';

import Connector from '../../../src/connectors/fetch';

describe('utils/fetch.js', () => {
  afterEach(sinon.restore);

  it('should fetch', (done) => {
    const stub = sinon.stub(Connector.prototype, 'fetch').resolves({ hello: 'world' });

    const uows = [{
      fetchRequest: {
        url: 'https://example.com',
      },
    }];

    _(uows)
      .through(fetch())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith('https://example.com', {}, 'json');

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          fetchRequest: {
            url: 'https://example.com',
          },
          fetchResponse: {
            hello: 'world',
          },
        });
      })
      .done(done);
  });
});
