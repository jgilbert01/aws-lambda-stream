import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { mockClient } from 'aws-sdk-client-mock';

import Connector from '../../../src/connectors/cloudwatch';

describe('connectors/cloudwatch.js', () => {
  let mockCloudWatch;

  beforeEach(() => {
    mockCloudWatch = mockClient(CloudWatchClient);
  });

  afterEach(() => {
    mockCloudWatch.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should put', async () => {
    const spy = sinon.spy((_) => ({}));
    mockCloudWatch.on(PutMetricDataCommand).callsFake(spy);

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
          Name: 'pipeline',
          Value: 'p1',
        }, {
          Name: 'type',
          Value: 'thing-created',
        },
      ],
    }];

    const data = await new Connector({ debug: debug('cw') })
      .put({ Namespace: 'ns', MetricData });

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      Namespace: 'ns',
      MetricData,
    });
    expect(data).to.deep.equal({});
  });
});
