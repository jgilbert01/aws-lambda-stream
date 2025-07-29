import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  toBatchUow, unBatchUow, group, batchWithSize, compact,
  batchWithPayloadSizeOrCount,
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

  it('should handle oversized requests', (done) => {
    const spy = sinon.spy();
    const uows = [
      {
        publishRequestEntry: { // size = 19
          id: 'xxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 39
          id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        },
      },
      {
        publishRequestEntry: { // size = 29
          id: 'xxxxxxxxxxxxxxxxxxxx',
        },
      },
    ];

    _(uows)
      .consume(batchWithSize({
        batchSize: 2,
        maxRequestSize: 30,
        requestEntryField: 'publishRequestEntry',
        // metricsEnabled: true,
        // debug: (msg, v) => console.log(msg, v),
      }))
      .errors(spy)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(2);
        expect(spy).to.have.been.calledWith; // (Error('Request size: 39, exceeded max: 30'));
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

  it('should compact', (done) => {
    const uows = [
      {
        debug: 'x',
        pipeline: 'y',
        event: {
          partitionKey: '1',
          timestamp: 2,
        },
      },
      {
        debug: 'x',
        pipeline: 'y',
        event: {
          partitionKey: '2',
          timestamp: 3,
        },
      },
      {
        debug: 'x',
        pipeline: 'y',
        event: {
          partitionKey: '1',
          timestamp: 1,
        },
      },
    ];

    _(uows)
      .through(compact({ compact: true }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected).to.deep.equal([
          {
            debug: 'x',
            pipeline: 'y',
            event: {
              partitionKey: '1',
              timestamp: 2,
            },
            batch: [
              {
                debug: 'x',
                pipeline: 'y',
                event: {
                  partitionKey: '1',
                  timestamp: 1,
                },
              },
              {
                debug: 'x',
                pipeline: 'y',
                event: {
                  partitionKey: '1',
                  timestamp: 2,
                },
              },
            ],
          },
          {
            debug: 'x',
            pipeline: 'y',
            event: {
              partitionKey: '2',
              timestamp: 3,
            },
            batch: [
              {
                debug: 'x',
                pipeline: 'y',
                event: {
                  partitionKey: '2',
                  timestamp: 3,
                },
              },
            ],
          },
        ]);
      })
      .done(done);
  });

  describe('batchWithPayloadSizeOrCount', () => {
    it('should batch on size', (done) => {
      const uows = [
        {
          message: { // size = 19
            id: 'xxxxxxxxxx',
          },
        },
        {
          message: { // size = 29
            id: 'xxxxxxxxxxxxxxxxxxxx',
          },
        },
        {
          message: { // size = 39
            id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
          },
        },
        {
          message: { // size = 10
            id: 'x',
          },
        },
      ];

      _(uows)
        .consume(batchWithPayloadSizeOrCount({
          batchSize: 999,
          maxPayloadSize: 50,
          payloadField: 'message',
        }))

        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(2);
          expect(collected[0]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxx',
              },
            },
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxx',
              },
            },
          ]);
          expect(collected[1]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
              },
            },
            {
              message: {
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
          message: { // size = 19
            id: 'xxxxxxxxxx',
          },
        },
        {
          message: { // size = 29
            id: 'xxxxxxxxxxxxxxxxxxxx',
          },
        },
        {
          message: { // size = 39
            id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
          },
        },
      ];

      _(uows)
        .consume(batchWithPayloadSizeOrCount({
          batchSize: 2,
          maxPayloadSize: 999,
          payloadField: 'message',
          // metricsEnabled: true,
          // debug: (msg, v) => console.log(msg, v),
        }))
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(2);
          expect(collected[0]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxx',
              },
            },
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxx',
              },
            },
          ]);
          expect(collected[1]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
              },
            },
          ]);
        })
        .done(done);
    });

    it('should handle oversized requests', (done) => {
      const spy = sinon.spy();
      const uows = [
        {
          message: { // size = 19
            id: 'xxxxxxxxxx',
          },
        },
        {
          message: { // size = 39
            id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
          },
        },
        {
          message: { // size = 29
            id: 'xxxxxxxxxxxxxxxxxxxx',
          },
        },
      ];

      _(uows)
        .consume(batchWithPayloadSizeOrCount({
          batchSize: 2,
          maxPayloadSize: 30,
          payloadField: 'message',
        }))
        .errors(spy)
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));
          expect(collected.length).to.equal(2);
          expect(spy).to.have.been.calledWith; // (Error('Request size: 39, exceeded max: 30'));
        })
        .done(done);
    });

    it('should skip batching nonexist payload fields', (done) => {
      const uows = [
        {
          message: { // size = 19
            id: 'xxxxxxxxxx',
          },
        },
        {
          message: { // size = 29
            id: 'xxxxxxxxxxxxxxxxxxxx',
          },
        },
        {
          message: { // size = 39
            id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
          },
        },
      ];

      _(uows)
        .consume(batchWithPayloadSizeOrCount({
          batchSize: 2,
          maxPayloadSize: 999,
          payloadField: 'fake-field',
          // metricsEnabled: true,
          // debug: (msg, v) => console.log(msg, v),
        }))
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(3);
          expect(collected[0]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxx',
              },
            },
          ]);
          expect(collected[1]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxx',
              },
            },
          ]);
          expect(collected[2]).to.deep.equal([
            {
              message: {
                id: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
              },
            },
          ]);
        })
        .done(done);
    });
  });
});
