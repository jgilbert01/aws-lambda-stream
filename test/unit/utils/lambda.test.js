import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { invokeLambda } from '../../../src/utils/lambda';

import Connector from '../../../src/connectors/lambda';

describe('utils/lambda.js', () => {
  afterEach(sinon.restore);

  it('should invoke lambda', (done) => {
    const stub = sinon.stub(Connector.prototype, 'invoke').resolves({});

    const uows = [{
      invokeRequest: {
        FunctionName: 'helloworld',
      },
    }];

    _(uows)
      .through(invokeLambda())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          FunctionName: 'helloworld',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          invokeRequest: {
            FunctionName: 'helloworld',
          },
          invokeResponse: {},
        });
      })
      .done(done);
  });
});
