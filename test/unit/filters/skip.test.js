import 'mocha';
import { expect } from 'chai';

import { fromKinesis, toKinesisRecords } from '../../../src';

import { outSkip, skipTag } from '../../../src/filters/skip';

describe('filters/skip.js', () => {
  afterEach(() => {
    // set back to state specified in package.json
    process.env.NODE_ENV = 'test';
  });

  it('should set skip tag', () => {
    process.env.NODE_ENV = 'test';
    expect(skipTag()).to.deep.equal({ skip: true });
  });

  it('should not set skip tag', () => {
    delete process.env.NODE_ENV;
    expect(skipTag()).to.deep.equal({ skip: undefined });

    process.env.NODE_ENV = 'other';
    expect(skipTag()).to.deep.equal({ skip: undefined });
  });

  it('should skip test events', (done) => {
    const event = toKinesisRecords([
      {
        type: 'tskip',
        partitionKey: 'tskip',
        tags: {
          skip: true,
        },
      },
      {
        type: 't1',
        partitionKey: '1',
      },
    ]);

    fromKinesis(event)
      .filter(outSkip)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].event).to.deep.equal({
          id: 'shardId-000000000000:1',
          type: 't1',
          partitionKey: '1',
        });
      })
      .done(done);
  });
});
