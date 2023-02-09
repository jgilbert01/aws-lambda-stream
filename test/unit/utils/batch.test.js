import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  toBatchUow, unBatchUow, group, batchWithSize,
} from '../../../src/utils';

describe('utils/batch.js', () => {
  afterEach(sinon.restore);

  it('should batch on size', (done) => {
    const uows = [
      {
        publishRequestEntry: { // size = 19
          id: 'xxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 29
          id: 'xxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 39
          id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 10
          id: 'x',
        },
      },
    ];

    _(uows)
      .consume(batchWithSize({
        batchSize: 999,
        maxRequestSize: 50,
        requestEntryField: 'publishRequestEntry',
        metricsEnabled: true,
        debug: (msg, v) => console.log(msg, v),
      }))

      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal([
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxx',
            },
          },
        ]);
        expect(collected[1]).to.deep.equal([
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'x',
            },
          },
        ]);
      })
      .done(done);
  });

  it('should batch on count', (done) => {
    const uows = [
      {
        publishRequestEntry: { // size = 19
          id: 'xxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 29
          id: 'xxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 39
          id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        },
      },
    ];

    _(uows)
      .consume(batchWithSize({
        batchSize: 2,
        maxRequestSize: 999,
        requestEntryField: 'publishRequestEntry',
        // metricsEnabled: true,
        // debug: (msg, v) => console.log(msg, v),
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal([
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxx',
            },
          },
        ]);
        expect(collected[1]).to.deep.equal([
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            },
          },
        ]);
      })
      .done(done);
  });

  it('should batch', (done) => {
    const uows = [
      {
        publishRequestEntry: {
          id: 'xxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: {
          id: 'xxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: {
          id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: {
          id: 'x',
        },
      },
    ];

    _(uows)
      .batch(2)
      .map(toBatchUow)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected).to.deep.equal([
          {
            batch: [
              {
                publishRequestEntry: {
                  id: 'xxxxxxxxxx',
                },
              },
              {
                publishRequestEntry: {
                  id: 'xxxxxxxxxxxxxxxxxxxx',
                },
              },
            ],
          },
          {
            batch: [
              {
                publishRequestEntry: {
                  id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
                },
              },
              {
                publishRequestEntry: {
                  id: 'x',
                },
              },
            ],
          },
        ]);
      })
      .done(done);
  });

  it('should unbatch', (done) => {
    const uows = [
      {
        batch: [
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxx',
            },
          },
        ],
      },
      {
        batch: [
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'x',
            },
          },
        ],
      },
    ];

    _(uows)
      .flatMap(unBatchUow)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(4);
        expect(collected).to.deep.equal([
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            },
          },
          {
            publishRequestEntry: {
              id: 'x',
            },
          },
        ]);
      })
      .done(done);
  });

  it('should group', (done) => {
    const uows = [
      {
        event: {
          partitionKey: '1',
        },
      },
      {
        event: {
          partitionKey: '1',
        },
      },
      {
        event: {
          partitionKey: '2',
        },
      },
    ];

    _(uows)
      .through(group({ group: true }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected).to.deep.equal([
          {
            batch: [
              {
                event: {
                  partitionKey: '1',
                },
              },
              {
                event: {
                  partitionKey: '1',
                },
              },
            ],
          },
          {
            batch: [
              {
                event: {
                  partitionKey: '2',
                },
              },
            ],
          },
        ]);
      })
      .done(done);
  });
});
