import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { publishToConnections, disconnectConnections } from '../../../src/sinks/websocket';

import Connector from '../../../src/connectors/apigatewayclient';

describe('sinks/websocket.js', () => {
  afterEach(sinon.restore);

  it('should skip uow without connectionId', (done) => {
    const stub = sinon.stub(Connector.prototype, 'postToConnection');

    const uows = [{ message: { type: 'thing-updated' } }];

    _(uows)
      .through(publishToConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].postResponse).to.be.undefined;
        expect(stub).to.not.have.been.called;
      })
      .done(done);
  });

  it('should skip uow without message field', (done) => {
    const stub = sinon.stub(Connector.prototype, 'postToConnection');

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(publishToConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].postResponse).to.be.undefined;
        expect(stub).to.not.have.been.called;
      })
      .done(done);
  });

  it('should post to connection', (done) => {
    sinon.stub(Connector.prototype, 'postToConnection').resolves({});

    const uows = [{
      connectionId: 'conn-1',
      message: { type: 'thing-updated', data: { id: '1' } },
    }];

    _(uows)
      .through(publishToConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].postResponse).to.deep.equal({});
        expect(collected[0].connectionId).to.equal('conn-1');
      })
      .done(done);
  });

  it('should support custom messageField', (done) => {
    const stub = sinon.stub(Connector.prototype, 'postToConnection').resolves({});

    const uows = [{
      connectionId: 'conn-1',
      payload: 'raw string message',
    }];

    _(uows)
      .through(publishToConnections({ messageField: 'payload' }))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(stub).to.have.been.calledWith('conn-1', 'raw string message', uows[0]);
      })
      .done(done);
  });

  it('should handle 410 gone silently', (done) => {
    const err = new Error('Gone');
    err.$metadata = { httpStatusCode: 410 };
    sinon.stub(Connector.prototype, 'postToConnection').rejects(err);

    const uows = [{
      connectionId: 'stale-conn',
      message: { type: 'thing-updated', data: { id: '1' } },
    }];

    _(uows)
      .through(publishToConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].postResponse.statusCode).to.equal(410);
        expect(collected[0].postResponse.connectionId).to.equal('stale-conn');
      })
      .done(done);
  });

  it('should fault on non-410 errors', (done) => {
    const err = new Error('Internal Server Error');
    sinon.stub(Connector.prototype, 'postToConnection').rejects(err);

    const uows = [{
      connectionId: 'conn-1',
      message: { type: 'thing-updated', data: { id: '1' } },
    }];

    _(uows)
      .through(publishToConnections())
      .errors((e) => {
        expect(e.message).to.equal('Internal Server Error');
        expect(e.uow).to.not.be.undefined;
        done();
      })
      .resume();
  });
});

describe('sinks/websocket.js - disconnectConnections', () => {
  afterEach(sinon.restore);

  it('should skip uow without connectionId', (done) => {
    const stub = sinon.stub(Connector.prototype, 'deleteConnection');

    const uows = [{ someField: 'value' }];

    _(uows)
      .through(disconnectConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].deleteResponse).to.be.undefined;
        expect(stub).to.not.have.been.called;
      })
      .done(done);
  });

  it('should disconnect connection', (done) => {
    sinon.stub(Connector.prototype, 'deleteConnection').resolves({});

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(disconnectConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].deleteResponse).to.deep.equal({});
        expect(collected[0].connectionId).to.equal('conn-1');
      })
      .done(done);
  });

  it('should handle 410 gone silently', (done) => {
    const err = new Error('Gone');
    err.$metadata = { httpStatusCode: 410 };
    sinon.stub(Connector.prototype, 'deleteConnection').rejects(err);

    const uows = [{ connectionId: 'stale-conn' }];

    _(uows)
      .through(disconnectConnections())
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].deleteResponse.statusCode).to.equal(410);
        expect(collected[0].deleteResponse.connectionId).to.equal('stale-conn');
      })
      .done(done);
  });

  it('should fault on non-410 errors', (done) => {
    const err = new Error('Internal Server Error');
    sinon.stub(Connector.prototype, 'deleteConnection').rejects(err);

    const uows = [{ connectionId: 'conn-1' }];

    _(uows)
      .through(disconnectConnections())
      .errors((e) => {
        expect(e.message).to.equal('Internal Server Error');
        expect(e.uow).to.not.be.undefined;
        done();
      })
      .resume();
  });
});
