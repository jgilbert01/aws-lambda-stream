import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';
import { FirehoseClient, PutRecordBatchCommand } from '@aws-sdk/client-firehose';

import Connector from '../../../src/connectors/firehose';

import { debug } from '../../../src/utils';

describe('connectors/firehose.js', () => {
  let mockFirehose;

  beforeEach(() => {
    mockFirehose = mockClient(FirehoseClient);
  });

  afterEach(() => {
    mockFirehose.restore();
  });

  it('should put', async () => {
    const spy = sinon.spy((_) => ({}));
    mockFirehose.on(PutRecordBatchCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('firehose'),
      deliveryStreamName: 'ds1',
    }).putRecordBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      DeliveryStreamName: 'ds1',
      Records: inputParams.Records,
    });
    expect(data).to.deep.equal({});
  });
});
