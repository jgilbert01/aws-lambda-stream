import 'mocha';
import { expect } from 'chai';

import {
  fromKinesis, toKinesisRecords, fromDynamodb, toDynamodbRecords,
} from '../../../src';

import { outSourceIsSelf, outLatched } from '../../../src/filters/latch';

describe('filters/latch.js', () => {
  afterEach(() => {
    delete process.env.SERVERLESS_PROJECT;
  });

  it('should ignore its own events', (done) => {
    process.env.SERVERLESS_PROJECT = 'my-service';

    const event = toKinesisRecords([
      {
        tags: {
          source: 'my-service',
        },
      },
      {
        tags: {
          source: 'another-service',
        },
      },
    ]);

    fromKinesis(event)
      .filter(outSourceIsSelf)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event).to.deep.equal({
          id: 'shardId-000000000000:1',
          tags: {
            source: 'another-service',
          },
        });
      })
      .done(done);
  });

  it('should ignore latched INSERT record', (done) => {
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
          // no latched field
        },
      },
      // ignore this latched event
      {
        timestamp: 1572832690,
        keys: {
          pk: '2',
          sk: 'thing',
        },
        newImage: {
          pk: '2',
          sk: 'thing',
          // latch
          latched: true,
        },
      },
    ]);

    fromDynamodb(events)
      .filter(outLatched)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event.partitionKey).to.equal('1');
      })
      .done(done);
  });

  it('should ignore latched MODIFY record', (done) => {
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
          // no latched field
        },
        oldImage: {
          pk: '1',
          sk: 'thing',
          // was previously latched
          latched: true,
        },
      },
      // ignore this latched event
      {
        timestamp: 1572832690,
        keys: {
          pk: '2',
          sk: 'thing',
        },
        newImage: {
          pk: '2',
          sk: 'thing',
          // latch
          latched: true,
        },
        oldImage: {
          pk: '2',
          sk: 'thing',
          // was not previously latched
        },
      },
    ]);

    fromDynamodb(events)
      .filter(outLatched)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event.partitionKey).to.equal('1');
      })
      .done(done);
  });

  it('should ignore latched REMOVE record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        oldImage: {
          pk: '1',
          sk: 'thing',
          // no latched field
        },
      },
      // ignore this latched event
      {
        timestamp: 1572832690,
        keys: {
          pk: '2',
          sk: 'thing',
        },
        oldImage: {
          pk: '2',
          sk: 'thing',
          // latch
          latched: true,
        },
      },
    ]);

    fromDynamodb(events)
      .filter(outLatched)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event.partitionKey).to.equal('1');
      })
      .done(done);
  });
});
