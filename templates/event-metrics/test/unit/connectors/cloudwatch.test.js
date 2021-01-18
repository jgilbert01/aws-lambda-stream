import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import AWS from 'aws-sdk-mock';
import Promise from 'bluebird';

import Connector from '../../../src/connectors/cloudwatch';

AWS.Promise = Promise;

describe('connectors/cloudwatch.js', () => {
  afterEach(() => {
    AWS.restore('CloudWatch');
  });

  it('should put', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('CloudWatch', 'putMetricData', spy);

    const MetricData = [{
      MetricName: 'domain.event',
      Unit: 'Count',
      Value: 1,
      Dimensions: [
        {
          Name: 'account',
          Value: 'dev',
        }, {
          Name: 'region',
          Value: 'us-east-1',
        }, {
          Name: 'source',
          Value: 'service-x',
        }, {
          Name: 'function',
          Value: 'f1',
        }, {
          Name: 'type',
          Value: 'thing-created',
        },
      ],
    }];

    const data = await new Connector(debug('cw'))
      .put('ns', MetricData);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      Namespace: 'ns',
      MetricData,
    });
    expect(data).to.deep.equal({});
  });
});
