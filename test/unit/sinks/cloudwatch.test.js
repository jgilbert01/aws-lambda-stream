import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { putMetrics } from '../../../src/sinks/cloudwatch';

import Connector from '../../../src/connectors/cloudwatch';

describe('utils/cloudwatch.js', () => {
  afterEach(sinon.restore);

  it('should putMetrics', (done) => {
    const stub = sinon.stub(Connector.prototype, 'put').resolves({});

    const uows = [{
      putRequest: {
        Namespace: 'ns',
      },
    }];

    _(uows)
      .through(putMetrics())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Namespace: 'ns',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          putRequest: {
            Namespace: 'ns',
          },
          putResponse: {},
        });
      })
      .done(done);
  });
});
