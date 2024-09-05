import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize } from '../../../src';

import { fromAlarm } from '../../../src/from/cw';

import Connector from '../../../src/connectors/lambda';

import { circuitBreaker } from '../../../src/flavors/circuitbreaker';

describe('flavors/circuitbreaker.js', () => {
  beforeEach(() => {
    process.env.ESM_ID = 'a092f90d-9948-4964-95b5-32c46093f734';
    sinon.stub(Connector.prototype, 'updateEventSourceMapping').resolves({});
  });

  afterEach(sinon.restore);

  it('should disable event source mapping (Circuit Breaker - Open)', (done) => {
    const event = {
      source: 'aws.cloudwatch',
      alarmArn: 'arn:aws:cloudwatch:us-east-1:444455556666:alarm:lambda-demo-metric-alarm',
      accountId: '444455556666',
      time: '2023-08-04T12:36:15.490+0000',
      region: 'us-east-1',
      alarmData: {
        alarmName: 'lambda-demo-metric-alarm',
        state: {
          value: 'ALARM',
        },
        previousState: {
          value: 'OK',
        },
      },
    };

    initialize({
      circuitBreaker,
    })
      .assemble(fromAlarm(event), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('circuitBreaker');
        expect(collected[0].updateRequest).to.deep.equal({
          UUID: 'a092f90d-9948-4964-95b5-32c46093f734',
          Enabled: false,
          BatchSize: 100,
        });
      })
      .done(done);
  });

  it('should enable event source mapping with batch size = 1 (Circuit Breaker - Part Open)', (done) => {
    const event = {
      source: 'aws.cloudwatch',
      alarmArn: 'arn:aws:cloudwatch:us-east-1:444455556666:alarm:lambda-demo-metric-alarm',
      accountId: '444455556666',
      time: '2023-08-04T12:36:15.490+0000',
      region: 'us-east-1',
      alarmData: {
        alarmName: 'lambda-demo-metric-alarm',
        state: {
          value: 'INSUFFICIENT_DATA',
        },
        previousState: {
          value: 'ALARM',
        },
      },
    };

    initialize({
      circuitBreaker,
    })
      .assemble(fromAlarm(event), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('circuitBreaker');
        expect(collected[0].updateRequest).to.deep.equal({
          UUID: 'a092f90d-9948-4964-95b5-32c46093f734',
          Enabled: true,
          BatchSize: 1,
        });
      })
      .done(done);
  });

  it('should enable event source mapping with full batch size (Circuit Breaker - Closed)', (done) => {
    const event = {
      source: 'aws.cloudwatch',
      alarmArn: 'arn:aws:cloudwatch:us-east-1:444455556666:alarm:lambda-demo-metric-alarm',
      accountId: '444455556666',
      time: '2023-08-04T12:36:15.490+0000',
      region: 'us-east-1',
      alarmData: {
        alarmName: 'lambda-demo-metric-alarm',
        state: {
          value: 'OK',
        },
        previousState: {
          value: 'INSUFFICIENT_DATA',
        },
      },
    };

    initialize({
      circuitBreaker,
    })
      .assemble(fromAlarm(event), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('circuitBreaker');
        expect(collected[0].updateRequest).to.deep.equal({
          UUID: 'a092f90d-9948-4964-95b5-32c46093f734',
          Enabled: true,
          BatchSize: 100,
        });
      })
      .done(done);
  });
});
