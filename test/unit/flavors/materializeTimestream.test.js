import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
  ttl,
} from '../../../src';

import { toKinesisRecords, fromKinesis } from '../../../src/from/kinesis';

import Connector from '../../../src/connectors/timestream';

import { materializeTimestream } from '../../../src/flavors/materializeTimestream';

describe('flavors/materializeTimestream.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'writeRecords').resolves({});
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    const events = toKinesisRecords([
      {
        type: 'thing-submitted',
        timestamp: 1548967022000,
        thing: {
          id: '1',
          status: 's1',
        },
      },
      {
        type: 'thing-submitted',
        timestamp: 1548967022000,
        thing: {
          id: '2',
          status: 's1',
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('m1');
        expect(collected[0].event.type).to.equal('thing-submitted');
        expect(collected[0].writeRequest).to.deep.equal({
          DatabaseName: 'd1',
          TableName: 't1',
          Records: [
            {
              Dimensions: [
                {
                  Name: 'type',
                  Value: 'thing-submitted',
                },
                {
                  Name: 'status',
                  Value: 's1',
                },
              ],
              MeasureName: 'domain.event',
              MeasureValue: '1',
              MeasureValueType: 'BIGINT',
              Time: '1548967022000',
              TimeUnit: 'MILLISECONDS',
            },
          ],
        });
      })
      .done(done);
  });
});

const toWriteRequest = (uow) => ({
  DatabaseName: 'd1',
  TableName: 't1',
  Records: [{
    Dimensions: [
      {
        Name: 'type',
        Value: uow.event.type,
      }, {
        Name: 'status',
        Value: uow.event.thing.status,
      },
    ],
    MeasureName: 'domain.event',
    MeasureValue: '1',
    MeasureValueType: 'BIGINT',
    Time: `${uow.event.timestamp}`,
    TimeUnit: 'MILLISECONDS',
  }],
});

const rules = [
  {
    id: 'm1',
    flavor: materializeTimestream,
    eventType: 'thing-submitted',
    toWriteRequest,
  },
];
