import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';

import Connector from '../../../src/connectors/kinesis';

import { debug } from '../../../src/utils';

describe('connectors/kinesis.js', () => {
  let mockKinesis;

  beforeEach(() => {
    mockKinesis = mockClient(KinesisClient);
  });

  afterEach(() => {
    mockKinesis.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should publish', async () => {
    const spy = sinon.spy((_) => ({ Records: [{ SequenceNumber: '1' }], FailedRecordCount: 0 }));
    mockKinesis.on(PutRecordsCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
          PartitionKey: '1',
        },
      ],
    };

    const data = await new Connector({
      debug: debug('kinesis'),
      streamName: 's1',
    }).putRecords(inputParams);

    expect(spy).to.have.been.calledWith({
      StreamName: 's1',
      Records: inputParams.Records,
    });
    expect(data).to.deep.equal({ Records: [{ SequenceNumber: '1' }], FailedRecordCount: 0 });
  });

  it('should retry', async () => {
    const responses = [
      { Records: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedRecordCount: 2 },
      { Records: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }], FailedRecordCount: 1 },
      { Records: [{ SequenceNumber: '3' }], FailedRecordCount: 0 },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockKinesis.on(PutRecordsCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
          PartitionKey: '1',
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't2' })),
          PartitionKey: '1',
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't3' })),
          PartitionKey: '1',
        },
      ],
    };

    const data = await new Connector({
      debug: debug('kinesis'),
      streamName: 's1',
    }).putRecords(inputParams);

    expect(spy).to.have.been.calledWith({
      Records: [inputParams.Records[0], inputParams.Records[1], inputParams.Records[2]],
      StreamName: 's1',
    });
    expect(spy).to.have.been.calledWith({
      Records: [inputParams.Records[1], inputParams.Records[2]],
      StreamName: 's1',
    });
    expect(spy).to.have.been.calledWith({
      Records: [inputParams.Records[2]],
      StreamName: 's1',
    });

    expect(data).to.deep.equal({
      Records: [{ SequenceNumber: '1' }, { SequenceNumber: '2' }, { SequenceNumber: '3' }],
      FailedRecordCount: 0,
      attempts: [
        {
          Records: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }],
          FailedRecordCount: 2,
        },
        {
          Records: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }],
          FailedRecordCount: 1,
        },
        {
          Records: [{ SequenceNumber: '3' }],
          FailedRecordCount: 0,
        },
      ],
    });
  });

  it('should throw on max retry', async () => {
    const responses = [
      { Records: [{ SequenceNumber: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedRecordCount: 2 },
      { Records: [{ SequenceNumber: '2' }, { ErrorCode: 'X' }], FailedRecordCount: 1 },
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockKinesis.on(PutRecordsCommand).callsFake(spy);

    const inputParams = {
      Records: [
        {
          Data: Buffer.from(JSON.stringify({ type: 't1' })),
          PartitionKey: '1',
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't2' })),
          PartitionKey: '1',
        },
        {
          Data: Buffer.from(JSON.stringify({ type: 't3' })),
          PartitionKey: '1',
        },
      ],
    };

    await new Connector({
      debug: debug('kinesis'),
      streamName: 's1',
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).putRecords(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          Records: [inputParams.Records[0], inputParams.Records[1], inputParams.Records[2]],
          StreamName: 's1',
        });
        expect(spy).to.have.been.calledWith({
          Records: [inputParams.Records[1], inputParams.Records[2]],
          StreamName: 's1',
        });
        expect(spy).to.not.have.been.calledWith({
          Records: [inputParams.Records[2]],
          StreamName: 's1',
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
