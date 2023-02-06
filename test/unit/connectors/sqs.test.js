import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/sqs';

import { debug } from '../../../src/utils';

describe('connectors/sqs.js', () => {
  afterEach(() => {
    AWS.restore('SQS');
  });

  it('should send msg', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { Successful: [{ Id: '1' }] }));
    AWS.mock('SQS', 'sendMessageBatch', spy);

    const inputParams = {
      Entries: [
        {
          Id: '1',
          MessageBody: JSON.stringify({ f1: 'v1' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      queueUrl: 'q1',
    }).sendMessageBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      QueueUrl: 'q1',
      Entries: inputParams.Entries,
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
    AWS.mock('SQS', 'sendMessageBatch', spy);

    const inputParams = {
      Entries: [
        {
          Id: '1',
          MessageBody: JSON.stringify({ f1: 'v1' }),
        },
        {
          Id: '2',
          MessageBody: JSON.stringify({ f2: 'v2' }),
        },
        {
          Id: '3',
          MessageBody: JSON.stringify({ f3: 'v3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      queueUrl: 'q1',
    }).sendMessageBatch(inputParams);

    expect(spy).to.have.been.calledWith({
      Entries: [inputParams.Entries[0], inputParams.Entries[1], inputParams.Entries[2]],
      QueueUrl: 'q1',
    });
    expect(spy).to.have.been.calledWith({
      Entries: [inputParams.Entries[1], inputParams.Entries[2]],
      QueueUrl: 'q1',
    });
    expect(spy).to.have.been.calledWith({
      Entries: [inputParams.Entries[2]],
      QueueUrl: 'q1',
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
    AWS.mock('SQS', 'sendMessageBatch', spy);

    const inputParams = {
      Entries: [
        {
          Id: '1',
          MessageBody: JSON.stringify({ f1: 'v1' }),
        },
        {
          Id: '2',
          MessageBody: JSON.stringify({ f2: 'v2' }),
        },
        {
          Id: '3',
          MessageBody: JSON.stringify({ f3: 'v3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('sqs'),
      queueUrl: 'q1',
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).sendMessageBatch(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          Entries: [inputParams.Entries[0], inputParams.Entries[1], inputParams.Entries[2]],
          QueueUrl: 'q1',
        });
        expect(spy).to.have.been.calledWith({
          Entries: [inputParams.Entries[1], inputParams.Entries[2]],
          QueueUrl: 'q1',
        });
        expect(spy).to.not.have.been.calledWith({
          Entries: [inputParams.Entries[2]],
          QueueUrl: 'q1',
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
