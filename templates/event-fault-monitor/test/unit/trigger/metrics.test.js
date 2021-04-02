import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { CloudWatchConnector as Connector } from 'aws-lambda-stream';

import pipeline from '../../../src/trigger/metrics';

describe('trigger/metrics.js', () => {
  afterEach(sinon.restore);

  it('should handle events', (done) => {
    const stub = sinon.stub(Connector.prototype, 'put')
      .resolves({});

    const uows = [{
      record: {},
      event: EVENT,
    }];

    _(uows)
      .through(pipeline())

      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(stub).to.have.been.calledOnce;

        // expect(stub).to.have.been.calledWith({
        //   Prefix: 'p1',
        //   Marker: undefined,
        // });

        // TODO Namespace
        expect(collected[0].putRequest.MetricData[0]).to.deep.equal({
          MetricName: 'domain.event',
          Timestamp: 1441121600,
          Unit: 'Count',
          Value: 1,
          Dimensions: [
            {
              Name: 'account',
              Value: 'dev',
            },
            {
              Name: 'region',
              Value: 'us-west-2',
            },
            {
              Name: 'stage',
              Value: 'not-specified',
            },
            {
              Name: 'source',
              Value: 'not-specified',
            },
            {
              Name: 'functionname',
              Value: 'f1',
            },
            {
              Name: 'pipeline',
              Value: 'p1',
            },
            {
              Name: 'type',
              Value: 'fault',
            },
          ],
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
