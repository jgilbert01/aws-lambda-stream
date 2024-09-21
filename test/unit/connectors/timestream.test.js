import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { TimestreamWriteClient, WriteRecordsCommand } from '@aws-sdk/client-timestream-write';
import { mockClient } from 'aws-sdk-client-mock';

import Connector from '../../../src/connectors/timestream';

describe('connectors/timestream.js', () => {
  let mockCloudWatch;

  beforeEach(() => {
    mockCloudWatch = mockClient(TimestreamWriteClient);
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

  it('should write', async () => {
    const spy = sinon.spy((_) => ({
      RecordsIngested: {
        Total: 1,
      },
    }));
    mockCloudWatch.on(WriteRecordsCommand).callsFake(spy);

    const params = {
      DatabaseName: 'd1',
      TableName: 't1',
      Records: [{
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
        MeasureName: 'domain.event',
        MeasureValue: '1',
        MeasureValueType: 'BIGINT',
        Time: '1726940256001',
        TimeUnit: 'MILLISECONDS',
      }],
    };
    const data = await new Connector({ debug: debug('cw') })
      .writeRecords(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      DatabaseName: 'd1',
      TableName: 't1',
      Records: [{
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
        MeasureName: 'domain.event',
        MeasureValue: '1',
        MeasureValueType: 'BIGINT',
        Time: '1726940256001',
        TimeUnit: 'MILLISECONDS',
      }],
    });
    expect(data).to.deep.equal({
      RecordsIngested: {
        Total: 1,
      },
    });
  });
});
