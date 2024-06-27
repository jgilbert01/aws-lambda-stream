import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';

import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import Connector from '../../../src/connectors/eventbridge';

import { debug } from '../../../src/utils';

describe('connectors/eventbridge.js', () => {
  let mockEventBridge;

  beforeEach(() => {
    mockEventBridge = mockClient(EventBridgeClient);
  });

  afterEach(() => {
    mockEventBridge.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should publish', async () => {
    const spy = sinon.spy((_) => ({ Entries: [{ EventId: '1' }], FailedEntryCount: 0 }));
    mockEventBridge.on(PutEventsCommand).callsFake(spy);

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

    const spy = sinon.spy((_) => responses.shift());
    mockEventBridge.on(PutEventsCommand).callsFake(spy);

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
      Entries: [inputParams.Entries[0], inputParams.Entries[1], inputParams.Entries[2]],
    });
    expect(spy).to.have.been.calledWith({
      Entries: [inputParams.Entries[1], inputParams.Entries[2]],
    });
    expect(spy).to.have.been.calledWith({
      Entries: [inputParams.Entries[2]],
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
    ];

    const spy = sinon.spy((_) => responses.shift());
    mockEventBridge.on(PutEventsCommand).callsFake(spy);

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

    await new Connector({
      debug: debug('eventbridge'),
      retryConfig: {
        maxRetries: 1,
        retryWait: 100,
      },
    }).putEvents(inputParams)
      .then(() => {
        expect.fail('should have thrown');
      }).catch((err) => {
        expect(spy).to.have.been.calledWith({
          Entries: [inputParams.Entries[0], inputParams.Entries[1], inputParams.Entries[2]],
        });
        expect(spy).to.have.been.calledWith({
          Entries: [inputParams.Entries[1], inputParams.Entries[2]],
        });
        expect(spy).to.not.have.been.calledWith({
          Entries: [inputParams.Entries[2]],
        });

        expect(err.message).to.contain('Failed batch requests');
      });
  });
});
