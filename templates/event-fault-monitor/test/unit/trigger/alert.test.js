import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { SnsConnector as Connector } from 'aws-lambda-stream';

import pipeline from '../../../src/trigger/alert';

describe('trigger/alert.js', () => {
  afterEach(sinon.restore);

  it('should handle events', (done) => {
    const stub = sinon.stub(Connector.prototype, 'publish')
      .resolves({});

    const uows = [{
      event: EVENT,
    }, {
      event: EVENT,
    }];

    _(uows)
      .through(pipeline())

      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(stub).to.have.been.calledWith({
          Subject: 'Fault: dev,us-west-2,stg,f1,p1',
          Message: JSON.stringify(EVENT, null, 2),
        });

        expect(collected[0].message).to.deep.equal({
          Subject: 'Fault: dev,us-west-2,stg,f1,p1',
          Message: JSON.stringify(EVENT, null, 2),
        });
      })
      .done(done);
  });
});

const EVENT = {
  type: 'fault',
  timestamp: 1441121600000,
  tags: {
    account: 'dev',
    region: 'us-west-2',
    stage: 'stg',
    functionname: 'f1',
    pipeline: 'p1',
  },
  err: {
    name: 'Error',
    message: 'this is an error',
    stack: 'the stack trace',
  },
  uow: {},
};
