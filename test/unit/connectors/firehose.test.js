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

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
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

  it('should retry', async () => {
    const responses = [
      { RequestResponses: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedPutCount: 2 },
      { RequestResponses: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }], FailedPutCount: 1 },
      { RequestResponses: [{ SequenceNumber: '3' }], FailedPutCount: 0 },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockFirehose.on(PutRecordBatchCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't2' })),
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't3' })),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('firehose'),
      deliveryStreamName: 'ds1',
    }).putRecordBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      DeliveryStreamName: 'ds1',
      Records: [inputParams.Records[0], inputParams.Records[1], inputParams.Records[2]],
    });
    expect(spy).to.have.been.calledWith({
      DeliveryStreamName: 'ds1',
      Records: [inputParams.Records[1], inputParams.Records[2]],
    });
    expect(spy).to.have.been.calledWith({
      DeliveryStreamName: 'ds1',
      Records: [inputParams.Records[2]],
    });

    expect(data).to.deep.equal({
      RequestResponses: [{ SequenceNumber: '1' }, { SequenceNumber: '2' }, { SequenceNumber: '3' }],
      FailedPutCount: 0,
      attempts: [
        {
          RequestResponses: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }],
          FailedPutCount: 2,
        },
        {
          RequestResponses: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }],
          FailedPutCount: 1,
        },
        {
          RequestResponses: [{ SequenceNumber: '3' }],
          FailedPutCount: 0,
        },
      ],
    });
  });

  it('should throw on max retry', async () => {
    const responses = [
      { RequestResponses: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedPutCount: 2 },
      { RequestResponses: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }], FailedPutCount: 1 },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockFirehose.on(PutRecordBatchCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't2' })),
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't3' })),
        },
      ],
    };

    await new Connector({
      debug: debug('firehose'),
      deliveryStreamName: 'ds1',
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).putRecordBatch(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          DeliveryStreamName: 'ds1',
          Records: [inputParams.Records[0], inputParams.Records[1], inputParams.Records[2]],
        });
        expect(spy).to.have.been.calledWith({
          DeliveryStreamName: 'ds1',
          Records: [inputParams.Records[1], inputParams.Records[2]],
        });
        expect(spy).to.not.have.been.calledWith({
          DeliveryStreamName: 'ds1',
          Records: [inputParams.Records[2]],
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
