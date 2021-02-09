import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/firehose';

import { debug } from '../../../src/utils';

describe('connectors/firehose.js', () => {
  afterEach(() => {
    AWS.restore('Firehose');
  });

  it('should put', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('Firehose', 'putRecordBatch', spy);

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
