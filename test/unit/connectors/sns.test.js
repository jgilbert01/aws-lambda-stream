import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/sns';

import { debug } from '../../../src/utils';

describe('connectors/sns.js', () => {
  afterEach(() => {
    AWS.restore('SNS');
  });

  it('should publish msg', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('SNS', 'publish', spy);

    const inputParams = {
      Message: JSON.stringify({ f1: 'v1' }),
    };

    const data = await new Connector({
      debug: debug('sns'),
      topicArn: 't1',
    }).publish(inputParams);

    expect(spy).to.have.been.calledWith({
      TopicArn: 't1',
      Message: inputParams.Message,
    });
    expect(data).to.deep.equal({});
  });

  it('should publish batch msg', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { Successful: [{ Id: '1' }] }));
    AWS.mock('SNS', 'publishBatch', spy);

    const inputParams = {
      PublishBatchRequestEntries: [
        {
          Id: '1',
          Message: JSON.stringify({ f1: 'v1' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      topicArn: 't1',
    }).publishBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      TopicArn: 't1',
      PublishBatchRequestEntries: inputParams.PublishBatchRequestEntries,
    });
    expect(data).to.deep.equal({ Successful: [{ Id: '1' }] });
  });

  it('should retry', async () => {
    const responses = [
      { Successful: [{ Id: '1' }], Failed: [{ Id: '2' }, { Id: '3' }] },
      { Successful: [{ Id: '2' }], Failed: [{ Id: '3' }] },
      { Successful: [{ Id: '3' }] },
    ];

    const spy = sinon.spy((params, cb) => cb(null, responses.shift()));
    AWS.mock('SNS', 'publishBatch', spy);

    const inputParams = {
      PublishBatchRequestEntries: [
        {
          Id: '1',
          Message: JSON.stringify({ f1: 'v1' }),
        },
        {
          Id: '2',
          Message: JSON.stringify({ f2: 'v2' }),
        },
        {
          Id: '3',
          Message: JSON.stringify({ f3: 'v3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      topicArn: 't1',
    }).publishBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[0], inputParams.PublishBatchRequestEntries[1], inputParams.PublishBatchRequestEntries[2]],
      TopicArn: 't1',
    });
    expect(spy).to.have.been.calledWith({
      PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[1], inputParams.PublishBatchRequestEntries[2]],
      TopicArn: 't1',
    });
    expect(spy).to.have.been.calledWith({
      PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[2]],
      TopicArn: 't1',
    });

    expect(data).to.deep.equal({
      Successful: [{ Id: '1' }, { Id: '2' }, { Id: '3' }],
      attempts: [
        {
          Successful: [{ Id: '1' }],
          Failed: [{ Id: '2' }, { Id: '3' }],
        },
        {
          Successful: [{ Id: '2' }],
          Failed: [{ Id: '3' }],
        },
        {
          Successful: [{ Id: '3' }],
        },
      ],
    });
  });

  it('should throw on max retry', async () => {
    const responses = [
      { Successful: [{ Id: '1' }], Failed: [{ Id: '2' }, { Id: '3' }] },
      { Successful: [{ Id: '2' }], Failed: [{ Id: '3' }] },
    ];

    const spy = sinon.spy((params, cb) => cb(null, responses.shift()));
    AWS.mock('SNS', 'publishBatch', spy);

    const inputParams = {
      PublishBatchRequestEntries: [
        {
          Id: '1',
          Message: JSON.stringify({ f1: 'v1' }),
        },
        {
          Id: '2',
          Message: JSON.stringify({ f2: 'v2' }),
        },
        {
          Id: '3',
          Message: JSON.stringify({ f3: 'v3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      topicArn: 't1',
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).publishBatch(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[0], inputParams.PublishBatchRequestEntries[1], inputParams.PublishBatchRequestEntries[2]],
          TopicArn: 't1',
        });
        expect(spy).to.have.been.calledWith({
          PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[1], inputParams.PublishBatchRequestEntries[2]],
          TopicArn: 't1',
        });
        expect(spy).to.not.have.been.calledWith({
          PublishBatchRequestEntries: [inputParams.PublishBatchRequestEntries[2]],
          TopicArn: 't1',
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
