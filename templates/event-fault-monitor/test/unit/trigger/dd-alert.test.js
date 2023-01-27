import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';
import moment from 'moment';
import { defaultOptions } from 'aws-lambda-stream';

import Connector from '../../../src/connectors/datadog';

import pipeline from '../../../src/trigger/dd-alert';

describe('trigger/dd-alert.js', () => {
  afterEach(sinon.restore);

  it('should handle events', (done) => {
    sinon.stub(moment.prototype, 'unix').returns(1645645918);
    const stub = sinon.stub(Connector.prototype, 'sendEvent')
      .resolves({});

    const uows = [{
      event: EVENT,
    }, {
      event: EVENT,
    }];

    _(uows)
      .through(pipeline(defaultOptions))

      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(stub).to.have.been.calledWith(
          'Fault Event: this is an error',
          'the stack trace',
          {
            alert_type: 'error',
            priority: 'all',
            date_happened: 1645645918,
            source_type_name: 'my apps',
            tags: [
              'event_type:fault',
              'account:undefined',
              'service:s1',
              'env:stg',
              'functionname:f1',
              'pipeline:p1',
              'error_type:Error',
            ],
          },
        );

        expect(collected[0].params).to.deep.equal({
          key: 'dev,us-west-2,stg,f1,p1',
          title: 'Fault Event: this is an error',
          properties: {
            alert_type: 'error',
            priority: 'all',
            date_happened: 1645645918,
            source_type_name: 'my apps',
            tags: [
              'event_type:fault',
              'account:undefined',
              'service:s1',
              'env:stg',
              'functionname:f1',
              'pipeline:p1',
              'error_type:Error',
            ],
          },
          message: 'the stack trace',
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
    source: 's1',
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
