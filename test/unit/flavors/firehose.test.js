import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  initialize, initializeFrom,
} from '../../../src';

import { toFirehoseRecords, fromFirehose } from '../../../src/from/firehose';
import { firehoseTransform, firehoseDrop } from '../../../src/flavors/firehose';

describe('flavors/firehose.js', () => {
  beforeEach(() => {
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    const events = toFirehoseRecords([
      {
        type: 't1',
        timestamp: 1734754684001,
        thing: {
          id: '1',
          name: 'Thing One',
          description: 'This is thing one',
        },
      },
      {
        type: 't2',
        timestamp: 1734754684001,
        thing: {
          id: '2',
          name: 'Thing Two',
          description: 'This is thing two',
        },
      },
      {
        type: 't3', // should be dropped
        timestamp: 1734754684001,
      },
    ]);

    initialize({
      ...initializeFrom(rules),
      drop: firehoseDrop(rules),
    })
      .assemble(fromFirehose(events), false)
      .collect()
      // .tap((records) => console.log(JSON.stringify(records, null, 2)))
      .tap((records) => {
        expect(records.length).to.equal(3);
        expect(records[0].pipeline).to.equal('ft1');
        expect(records[0].event.type).to.equal('t1');
        expect(records[0].transformed).to.deep.equal({
          ID: '1',
          NM: 'Thing One',
          DESC: 'This is thing one',
        });
        expect(records[0].metadata).to.deep.equal({
          partitionKeys: {
            table: 'T1',
            year: '2024',
            month: '12',
            day: '21',
            hour: '04',
            minute: '18',
          },
        });
        expect(records[0].data).to.equal('eyJJRCI6IjEiLCJOTSI6IlRoaW5nIE9uZSIsIkRFU0MiOiJUaGlzIGlzIHRoaW5nIG9uZSJ9');
        expect(records[0].result).to.equal('Ok');
        expect(records[1].result).to.equal('Ok');
        expect(records[2].result).to.equal('Dropped');
      })
      .done(done);
  });
});

const transform = (uow) => ({
  ID: uow.event.thing.id,
  NM: uow.event.thing.name,
  DESC: uow.event.thing.description,
});

const rules = [
  {
    id: 'ft1',
    flavor: firehoseTransform,
    eventType: 't1',
    tableName: 'T1',
    transform,
  },
  {
    id: 'ft2',
    flavor: firehoseTransform,
    eventType: 't2',
    filters: [() => true],
    tableName: 'T2',
    transform,
  },
  {
    id: 'other1',
    eventType: 'x9',
    flavor: firehoseTransform,
  },
];
