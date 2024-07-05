import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { formatMetrics } from '../../../src/metrics/emf';
import Timer from '../../../src/metrics/timer';

describe('metrics/emf.js', () => {
  beforeEach(() => {
    sinon.stub(Timer, 'now').returns(1720069357686);
    process.env.NAMESPACE = 'test';
    // process.env.ENABLE_EMF = 'true';
  });
  afterEach(() => {
    sinon.restore();
    delete process.env.NAMESPACE;
    // delete process.env.ENABLE_EMF;
  });

  it('should log metrics in emf format', () => {
    const emf = formatMetrics(METRICS);
    // console.log('emf: ', JSON.stringify(emf, null, 2));
    expect(emf).to.deep.equal([
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'stream.batch.utilization': 0.6,
        'stream.uow.count': 4,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.batch.utilization',
                  Unit: 'Percent',
                },
                {
                  Name: 'stream.uow.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p1',
        'stream.pipeline.utilization': 0.75,
        'stream.channel.wait.time.average': 2010,
        'stream.channel.wait.time.min': 2002,
        'stream.channel.wait.time.max': 2018,
        'stream.channel.wait.time.sum': 6030,
        'stream.channel.wait.time.count': 3,
        'stream.pipeline.time.average': 2044,
        'stream.pipeline.time.min': 2042,
        'stream.pipeline.time.max': 2046,
        'stream.pipeline.time.sum': 6132,
        'stream.pipeline.time.count': 3,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.utilization',
                  Unit: 'Percent',
                },
                {
                  Name: 'stream.channel.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p2',
        'stream.pipeline.utilization': 0.25,
        'stream.channel.wait.time.average': 2012,
        'stream.channel.wait.time.min': 2012,
        'stream.channel.wait.time.max': 2012,
        'stream.channel.wait.time.sum': 2012,
        'stream.channel.wait.time.count': 1,
        'stream.pipeline.time.average': 2054,
        'stream.pipeline.time.min': 2054,
        'stream.pipeline.time.max': 2054,
        'stream.pipeline.time.sum': 2054,
        'stream.pipeline.time.count': 1,
        'stream.pipeline.compact.count.average': 2,
        'stream.pipeline.compact.count.min': 2,
        'stream.pipeline.compact.count.max': 2,
        'stream.pipeline.compact.count.sum': 2,
        'stream.pipeline.compact.count.count': 1,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.utilization',
                  Unit: 'Percent',
                },
                {
                  Name: 'stream.channel.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.channel.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.compact.count.average',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.compact.count.min',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.compact.count.max',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.compact.count.sum',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.compact.count.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p1',
        'step': 'save',
        'stream.pipeline.io.wait.time.average': 20,
        'stream.pipeline.io.wait.time.min': 14,
        'stream.pipeline.io.wait.time.max': 26,
        'stream.pipeline.io.wait.time.sum': 60,
        'stream.pipeline.io.wait.time.count': 3,
        'stream.pipeline.io.time.average': 8,
        'stream.pipeline.io.time.min': 8,
        'stream.pipeline.io.time.max': 8,
        'stream.pipeline.io.time.sum': 24,
        'stream.pipeline.io.time.count': 3,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'step',
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.io.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.io.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p2',
        'step': 'query',
        'stream.pipeline.io.wait.time.average': 12,
        'stream.pipeline.io.wait.time.min': 12,
        'stream.pipeline.io.wait.time.max': 12,
        'stream.pipeline.io.wait.time.sum': 12,
        'stream.pipeline.io.wait.time.count': 1,
        'stream.pipeline.io.time.average': 2,
        'stream.pipeline.io.time.min': 2,
        'stream.pipeline.io.time.max': 2,
        'stream.pipeline.io.time.sum': 2,
        'stream.pipeline.io.time.count': 1,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'step',
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.io.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.io.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p2',
        'step': 'get',
        'stream.pipeline.io.wait.time.average': 8,
        'stream.pipeline.io.wait.time.min': 8,
        'stream.pipeline.io.wait.time.max': 8,
        'stream.pipeline.io.wait.time.sum': 8,
        'stream.pipeline.io.wait.time.count': 1,
        'stream.pipeline.io.time.average': 14,
        'stream.pipeline.io.time.min': 14,
        'stream.pipeline.io.time.max': 14,
        'stream.pipeline.io.time.sum': 14,
        'stream.pipeline.io.time.count': 1,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'step',
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.io.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.io.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
      {
        'account': 'undefined',
        'region': 'us-west-2',
        'stage': 'undefined',
        'source': 'undefined',
        'functionname': 'undefined',
        'pipeline': 'p2',
        'step': 'publish',
        'stream.pipeline.io.wait.time.average': 2,
        'stream.pipeline.io.wait.time.min': 2,
        'stream.pipeline.io.wait.time.max': 2,
        'stream.pipeline.io.wait.time.sum': 2,
        'stream.pipeline.io.wait.time.count': 1,
        'stream.pipeline.io.time.average': 2,
        'stream.pipeline.io.time.min': 2,
        'stream.pipeline.io.time.max': 2,
        'stream.pipeline.io.time.sum': 2,
        'stream.pipeline.io.time.count': 1,
        'stream.pipeline.batchSize.count.average': 1,
        'stream.pipeline.batchSize.count.min': 1,
        'stream.pipeline.batchSize.count.max': 1,
        'stream.pipeline.batchSize.count.sum': 1,
        'stream.pipeline.batchSize.count.count': 1,
        'stream.pipeline.eventSize.bytes.average': 365,
        'stream.pipeline.eventSize.bytes.min': 365,
        'stream.pipeline.eventSize.bytes.max': 365,
        'stream.pipeline.eventSize.bytes.sum': 365,
        'stream.pipeline.eventSize.bytes.count': 1,
        '_aws': {
          Timestamp: 1720069357686,
          CloudWatchMetrics: [
            {
              Namespace: 'test',
              Dimensions: [
                'step',
                'pipeline',
                'functionname',
                'source',
                'stage',
                'region',
                'account',
              ],
              Metrics: [
                {
                  Name: 'stream.pipeline.io.wait.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.wait.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.io.time.average',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.min',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.max',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.sum',
                  Unit: 'Milliseconds',
                },
                {
                  Name: 'stream.pipeline.io.time.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.batchSize.count.average',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.batchSize.count.min',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.batchSize.count.max',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.batchSize.count.sum',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.batchSize.count.count',
                  Unit: 'Count',
                },
                {
                  Name: 'stream.pipeline.eventSize.bytes.average',
                  Unit: 'Bytes',
                },
                {
                  Name: 'stream.pipeline.eventSize.bytes.min',
                  Unit: 'Bytes',
                },
                {
                  Name: 'stream.pipeline.eventSize.bytes.max',
                  Unit: 'Bytes',
                },
                {
                  Name: 'stream.pipeline.eventSize.bytes.sum',
                  Unit: 'Bytes',
                },
                {
                  Name: 'stream.pipeline.eventSize.bytes.count',
                  Unit: 'Count',
                },
              ],
            },
          ],
        },
      },
    ]);
  });
});

const METRICS = {
  'stream.batch.utilization': 0.6,
  'stream.uow.count': 4,
  'p1|stream.pipeline.utilization': 0.75,
  'p2|stream.pipeline.utilization': 0.25,
  'p1|stream.channel.wait.time': {
    average: 2010,
    min: 2002,
    max: 2018,
    sum: 6030,
    count: 3,
  },
  'p1|save|stream.pipeline.io.wait.time': {
    average: 20,
    min: 14,
    max: 26,
    sum: 60,
    count: 3,
  },
  'p1|save|stream.pipeline.io.time': {
    average: 8,
    min: 8,
    max: 8,
    sum: 24,
    count: 3,
  },
  'p1|stream.pipeline.time': {
    average: 2044,
    min: 2042,
    max: 2046,
    sum: 6132,
    count: 3,
  },
  'p2|stream.channel.wait.time': {
    average: 2012,
    min: 2012,
    max: 2012,
    sum: 2012,
    count: 1,
  },
  'p2|query|stream.pipeline.io.wait.time': {
    average: 12,
    min: 12,
    max: 12,
    sum: 12,
    count: 1,
  },
  'p2|query|stream.pipeline.io.time': {
    average: 2,
    min: 2,
    max: 2,
    sum: 2,
    count: 1,
  },
  'p2|get|stream.pipeline.io.wait.time': {
    average: 8,
    min: 8,
    max: 8,
    sum: 8,
    count: 1,
  },
  'p2|get|stream.pipeline.io.time': {
    average: 14,
    min: 14,
    max: 14,
    sum: 14,
    count: 1,
  },
  'p2|publish|stream.pipeline.io.wait.time': {
    average: 2,
    min: 2,
    max: 2,
    sum: 2,
    count: 1,
  },
  'p2|publish|stream.pipeline.io.time': {
    average: 2,
    min: 2,
    max: 2,
    sum: 2,
    count: 1,
  },
  'p2|stream.pipeline.time': {
    average: 2054,
    min: 2054,
    max: 2054,
    sum: 2054,
    count: 1,
  },
  'p2|stream.pipeline.compact.count': {
    average: 2,
    min: 2,
    max: 2,
    sum: 2,
    count: 1,
  },
  'p2|publish|stream.pipeline.batchSize.count': {
    average: 1,
    min: 1,
    max: 1,
    sum: 1,
    count: 1,
  },
  'p2|publish|stream.pipeline.eventSize.bytes': {
    average: 365,
    min: 365,
    max: 365,
    sum: 365,
    count: 1,
  },
};
