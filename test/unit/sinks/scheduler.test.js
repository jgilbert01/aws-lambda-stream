import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { scheduleEvent } from '../../../src/sinks/scheduler';

import Connector from '../../../src/connectors/scheduler';

describe('sinks/scheduler.js', () => {
  afterEach(sinon.restore);

  it('should schedule', (done) => {
    const stub = sinon.stub(Connector.prototype, 'schedule').resolves({});

    const uows = [{
      scheduleRequest: {
        X: 'y',
      },
    }];

    _(uows)
      .through(scheduleEvent())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          X: 'y',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          scheduleRequest: {
            X: 'y',
          },
          scheduleResponse: {},
        });
      })
      .done(done);
  });
});
