import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize } from '../../../src';

import { fromAlarm } from '../../../src/from/cw';

import Connector from '../../../src/connectors/lambda';

import { streamFailover } from '../../../src/flavors/streamFailover';

describe('flavors/streamFailover.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'listEventSourceMappings').resolves({
      EventSourceMappings: [{
        UUID: 'e1',
        Enabled: true,
        BatchSize: 42,
        EventSourceArn: 'override-me',
        FunctionName: 'override-me',
      }],
    });
    sinon.stub(Connector.prototype, 'createEventSourceMapping').resolves({ UUID: 'w1' });
    sinon.stub(Connector.prototype, 'deleteEventSourceMapping').resolves({ });
    sinon.stub(Connector.prototype, 'updateEventSourceMapping').resolves({});

    process.env.LISTENER_FUNCTION_NAME = 'my-listener';
    process.env.STREAM_ARN = 'arn:west:s1';
  });

  afterEach(sinon.restore);

  it('should move event source mapping between regions (fail-over)', (done) => {
    const event = {
      'version': '0',
      'id': 'c4c1c1c9-6542-e61b-6ef0-8c4d36933a92',
      'detail-type': 'CloudWatch Alarm State Change',
      'source': 'aws.cloudwatch',
      'account': '123456789012',
      'time': '2019-10-02T17:04:40Z',
      'region': 'us-east-1',
      'detail': {
        state: {
          value: 'ALARM',
        },
        previousState: {
          value: 'OK',
        },
      },
    };

    initialize({
      streamFailover,
    })
      .assemble(fromAlarm(event), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('streamFailover');
        expect(collected[0].listRequest).to.deep.equal({
          region: 'us-east-1',
          FunctionName: 'my-listener',
        });
        expect(collected[0].createRequest).to.deep.equal({
          region: 'us-west-2',
          UUID: undefined,
          Enabled: false,
          BatchSize: 42,
          EventSourceArn: 'arn:west:s1',
          FunctionName: 'my-listener',
          StartingPosition: 'AT_TIMESTAMP',
          StartingPositionTimestamp: 1570034080000,
        });
        expect(collected[0].deleteRequest).to.deep.equal({
          region: 'us-east-1',
          UUID: 'e1',
        });
        expect(collected[0].updateRequest).to.deep.equal({
          region: 'us-west-2',
          UUID: 'w1',
          Enabled: true,
        });
      })
      .done(done);
  });

  it('should move event source mapping between regions (fail-back)', (done) => {
    const event = {
      'version': '0',
      'id': 'c4c1c1c9-6542-e61b-6ef0-8c4d36933a92',
      'detail-type': 'CloudWatch Alarm State Change',
      'source': 'aws.cloudwatch',
      'account': '123456789012',
      'time': '2019-10-02T17:04:40Z',
      'region': 'us-west-2',
      'detail': {
        state: {
          value: 'OK',
        },
        previousState: {
          value: 'ALARM',
        },
      },
    };

    initialize({
      streamFailover,
    })
      .assemble(fromAlarm(event), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
      })
      .done(done);
  });
});
