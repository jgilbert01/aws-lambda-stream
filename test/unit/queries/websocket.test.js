import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { queryConnection } from '../../../src/queries/websocket';

import Connector from '../../../src/connectors/apigatewayclient';

describe('queries/websocket.js', () => {
  afterEach(sinon.restore);

  it('should skip uow without connectionId', (done) => {
    const stub = sinon.stub(Connector.prototype, 'getConnection');

    const uows = [{ someField: 'value' }];

    _(uows)
      .through(queryConnection())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].getConnectionResponse).to.be.undefined;
        expect(stub).to.not.have.been.called;
      })
      .done(done);
  });

  it('should get connection info', (done) => {
    const connectionInfo = { ConnectedAt: '2024-01-01T00:00:00Z', LastActiveAt: '2024-01-01T01:00:00Z' };
    sinon.stub(Connector.prototype, 'getConnection').resolves(connectionInfo);

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(queryConnection())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].connectionId).to.equal('conn-1');
        expect(collected[0].getConnectionResponse).to.deep.equal(connectionInfo);
      })
      .done(done);
  });

  it('should support custom response field name', (done) => {
    sinon.stub(Connector.prototype, 'getConnection').resolves({ ConnectedAt: '2024-01-01T00:00:00Z' });

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(queryConnection({ getConnectionResponseField: 'wsInfo' }))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].wsInfo).to.deep.equal({ ConnectedAt: '2024-01-01T00:00:00Z' });
      })
      .done(done);
  });

  it('should fault on error', (done) => {
    const err = new Error('Forbidden');
    sinon.stub(Connector.prototype, 'getConnection').rejects(err);

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(queryConnection())
      .errors((e) => {
        expect(e.message).to.equal('Forbidden');
        expect(e.uow).to.not.be.undefined;
        done();
      })
      .resume();
  });
});
