import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { startQueryExecution } from '../../../src/sinks/athena';

import Connector from '../../../src/connectors/athena';

describe('sinks/athena.js', () => {
  afterEach(sinon.restore);

  it('should start a query', (done) => {
    const stub = sinon.stub(Connector.prototype, 'startQueryExecution').resolves({});

    const uows = [{
      queryRequest: {
        X: 'y',
      },
    }];

    _(uows)
      .through(startQueryExecution({ id: 'p1' }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          ClientRequestToken: 'undefined-p1',
          WorkGroup: undefined,
          ResultConfiguration: {
            OutputLocation: undefined,
            EncryptionConfiguration: { EncryptionOption: 'CSE_KMS', KmsKey: 'kms-arn' },
          },
          QueryExecutionContext: { Database: undefined, Catalog: undefined },
          X: 'y',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          queryRequest: {
            X: 'y',
          },
          queryResponse: {},
        });
      })
      .done(done);
  });
});
