import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
} from '../../../src';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import Connector from '../../../src/connectors/scheduler';
import { scheduler } from '../../../src/flavors/scheduler';

describe('flavors/scheduler.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'schedule').resolves({});
  });

  afterEach(() => {
    sinon.restore();
  });

  it('should execute', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'Thing One',
          description: 'This is thing one',
          otherThing: 'thing|2',
          ttl: 1549053422,
          timestamp: 1548967022000,
        },
      },
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'other',
        },
        newImage: {
          pk: '1',
          sk: 'other',
          discriminator: 'other',
          name: 'Other One',
          description: 'This is other one',
          ttl: 1549053422,
          timestamp: 1548967022000,
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('schedule1');
        expect(collected[0].event.type).to.equal('thing-created');
        expect(collected[0].scheduleRequest).to.deep.equal({
          Name: 'test schedule',
          ActionAfterCompletion: 'DELETE',
          FlexibleTimeWindow: {
            Mode: 'OFF',
          },
        });
        expect(collected[0].scheduleResponse).to.deep.equal({});
      })
      .done(done);
  });
});

export const toScheduleRequest = (uow) => ({
  Name: 'test schedule',
  ActionAfterCompletion: 'DELETE',
  FlexibleTimeWindow: {
    Mode: 'OFF',
  },
});

const rules = [
  {
    id: 'schedule1',
    flavor: scheduler,
    eventType: /thing-*/,
    filters: [() => true],
    toScheduleRequest,
  },
  {
    id: 'update-other1',
    flavor: scheduler,
    eventType: 'x9',
  },
];
