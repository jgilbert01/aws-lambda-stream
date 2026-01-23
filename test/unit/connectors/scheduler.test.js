import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { mockClient } from 'aws-sdk-client-mock';

import {
  SchedulerClient,
  CreateScheduleCommand,
} from '@aws-sdk/client-scheduler';

import Connector from '../../../src/connectors/scheduler';

describe('connectors/scheduler.js', () => {
  let mockScheduler;

  beforeEach(() => {
    mockScheduler = mockClient(SchedulerClient);
  });

  afterEach(() => {
    mockScheduler.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should create schedule', async () => {
    process.env.SCHEDULER_ROLE_ARN = 'scheduler-arn';
    process.env.BUS_ARN = 'bus-arn';
    process.env.MASTER_KEY_ARN = 'kms-arn';

    const spy = sinon.spy((_) => ({
      ScheduleArn: `arn:aws:scheduler:${process.env.AWS_REGION}:123456789012:schedule/default/${params.Name}`,
    }));
    mockScheduler.on(CreateScheduleCommand).callsFake(spy);

    const event = {
      id: 'a33cbab0-d102-11ee-9c51-739469ab019e',
      timestamp: 1708554021000,
      type: 'test-event',
    };

    const params = {
      Name: `scheduled-${event.id}`,
      Target: {
        EventBridgeParameters: {
          DetailType: event.type,
          Source: process.env.SERVICE || 'undefined',
        },
        Input: JSON.stringify({
          ...event,
          type: `scheduled-${event.type}`,
        }),
      },
      FlexibleTimeWindow: {
        Mode: 'OFF',
      },
      Description: 'Test event scheduled',
      ScheduleExpression: 'at(2026-01-21T22:23:00)',
    };
    const data = await new Connector({ debug: debug('sched') })
      .schedule(params);

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      Description: 'Test event scheduled',
      FlexibleTimeWindow: {
        Mode: 'OFF',
      },
      Name: 'scheduled-a33cbab0-d102-11ee-9c51-739469ab019e',
      ScheduleExpression: 'at(2026-01-21T22:23:00)',
      Target: {
        Arn: 'bus-arn',
        EventBridgeParameters: {
          DetailType: 'test-event',
          Source: 'undefined',
        },
        Input: '{"id":"a33cbab0-d102-11ee-9c51-739469ab019e","timestamp":1708554021000,"type":"scheduled-test-event"}',
        RoleArn: 'scheduler-arn',
      },
      ActionAfterCompletion: 'DELETE',
      KmsKeyArn: 'kms-arn',
    });
    expect(data).to.deep.equal({
      ScheduleArn: 'arn:aws:scheduler:us-west-2:123456789012:schedule/default/scheduled-a33cbab0-d102-11ee-9c51-739469ab019e',
    });
  });
});
