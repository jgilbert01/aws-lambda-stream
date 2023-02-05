import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/eventbridge';

import { debug } from '../../../src/utils';

describe('connectors/eventbridge.js', () => {
  afterEach(() => {
    AWS.restore('EventBridge');
  });

  it('should publish', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { Entries: [{ EventId: '1' }], FailedEntryCount: 0 }));
    AWS.mock('EventBridge', 'putEvents', spy);

    const inputParams = {
      Entries: [
        {
          EventBusName: 'b1',
          DetailType: 't1',
          Detail: JSON.stringify({ type: 't1' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('eventbridge'),
    }).putEvents(inputParams);

    expect(spy).to.have.been.calledWith({
      Entries: inputParams.Entries,
    });
    expect(data).to.deep.equal({ Entries: [{ EventId: '1' }], FailedEntryCount: 0 });
  });

  it('should retry', async () => {
    const responses = [
      { Entries: [{ EventId: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedEntryCount: 2 },
      { Entries: [{ EventId: '2' }, { ErrorCode: 'X' }], FailedEntryCount: 1 },
      { Entries: [{ EventId: '3' }], FailedEntryCount: 0 },
    ];

    const spy = sinon.spy((params, cb) => cb(null, responses.shift()));
    AWS.mock('EventBridge', 'putEvents', spy);

    const inputParams = {
      Entries: [
        {
          EventBusName: 'b1',
          DetailType: 't1',
          Detail: JSON.stringify({ type: 't1' }),
        },
        {
          EventBusName: 'b1',
          DetailType: 't2',
          Detail: JSON.stringify({ type: 't2' }),
        },
        {
          EventBusName: 'b1',
          DetailType: 't3',
          Detail: JSON.stringify({ type: 't3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('eventbridge'),
    }).putEvents(inputParams);

    expect(spy).to.have.been.calledWith({
      Entries: inputParams.Entries,
    });
    expect(data).to.deep.equal({
      Entries: [{ EventId: '1' }, { EventId: '2' }, { EventId: '3' }],
      FailedEntryCount: 0,
      attempts: [
        {
          Entries: [{ EventId: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }],
          FailedEntryCount: 2,
        },
        {
          Entries: [{ EventId: '2' }, { ErrorCode: 'X' }],
          FailedEntryCount: 1,
        },
        {
          Entries: [{ EventId: '3' }],
          FailedEntryCount: 0,
        },
      ],
    });
  });

  it('should throw on max retry', async () => {
    const responses = [
      { Entries: [{ EventId: '1' }, { ErrorCode: 'X' }, { ErrorCode: 'X' }], FailedEntryCount: 2 },
      { Entries: [{ EventId: '2' }, { ErrorCode: 'X' }], FailedEntryCount: 1 },
      { Entries: [{ EventId: '3' }], FailedEntryCount: 0 },
    ];

    const spy = sinon.spy((params, cb) => cb(null, responses.shift()));
    AWS.mock('EventBridge', 'putEvents', spy);

    const inputParams = {
      Entries: [
        {
          EventBusName: 'b1',
          DetailType: 't1',
          Detail: JSON.stringify({ type: 't1' }),
        },
        {
          EventBusName: 'b1',
          DetailType: 't2',
          Detail: JSON.stringify({ type: 't2' }),
        },
        {
          EventBusName: 'b1',
          DetailType: 't3',
          Detail: JSON.stringify({ type: 't3' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('eventbridge'),
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).putEvents(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
