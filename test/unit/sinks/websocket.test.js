import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { publishToConnections } from '../../../src/sinks/websocket';

import Connector from '../../../src/connectors/websocket';

describe('sinks/websocket.js', () => {
  afterEach(sinon.restore);

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
