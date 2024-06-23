import _ from 'highland';
import Promise from 'bluebird';

import Timer from './timer';

let functionMetrics = {};

export const clear = () => {
  functionMetrics = {};
};

export const convertKinesisTs = (ts) => {
  if (!ts) return undefined;
  const time = `${ts}`;
  return Number(`${time.substring(0, 10)}${time.substring(11)}`);
};

export const startUow = (publishTime, batchSize) => {
  functionMetrics = {
    ...functionMetrics,
    'stream.batch.size': batchSize,
    'stream.batch.utilization': batchSize / Number(process.env.BATCH_SIZE || 100),
  };

  return new PipelineMetrics({
    publishTime,
  });
};

export class PipelineMetrics {
  constructor({
    pipeline, publishTime, timer,
  }) {
    this.pipeline = pipeline || 'default';
    this.timer = new Timer({
      start: publishTime || timer?.start,
      last: timer?.last,
      checkpoints: timer?.checkpoints,
    });
  }

  startPipeline({ pipeline }, pipelineCount) {
    const clone = new PipelineMetrics({
      pipeline,
      timer: this.timer,
    });

    // time waiting on channel capacity (e.g. shard count)
    clone.timer.checkpoint(`${pipeline}|stream.channel.wait.time`);

    if (pipelineCount) {
      functionMetrics['stream.pipeline.count'] = pipelineCount;
    }

    return clone;
  }

  endPipeline() {
    this.timer.end(`${this.pipeline}|stream.pipeline.time`);
    return this;
  }

  startStep(step) {
    // time waiting for io capacity (e.g parallel count)
    this.timer.checkpoint(`${this.pipeline}|${step}|stream.pipeline.io.wait.time`);
    return this;
  }

  endStep(step) {
    this.timer.checkpoint(`${this.pipeline}|${step}|stream.pipeline.io.time`);
    return this;
  }

  // wrap promise
  w(p, step) {
    const self = this;
    return new Promise((resolve, reject) => {
      // console.log('uow: ', uow);
      self.startStep(step);
      return p.then(resolve, reject)
        // TODO record capacity utilization ???
        .tap(self.endStep(step));
    });
  }
}

const calculateStats = (values) =>
  values.reduce((a, value, i) => ({
    ...a,
    average: (a.sum + value) / (i + 1),
    min: a.min < value ? a.min : value,
    max: a.max > value ? a.max : value,
    sum: a.sum + value,
    count: i + 1,
  }), {
    average: 0,
    min: undefined,
    max: undefined,
    sum: 0,
  });

const splitKey = (key) => {
  const [pipeline, step, metric] = key.split('|');
  return { pipeline, step, metric };
};

const calculateMetrics = (collected) => {
  // console.log('collected: ', JSON.stringify(collected.map((u) => u.metrics), null, 2));

  functionMetrics['stream.uow.count'] = collected.length;

  const checkpoints = collected
    .reduce((a, { metrics: { timer } }) => [
      ...a,
      ...Object.entries(timer.checkpoints)
        .map(([key, { value }]) => ({ key, value })),
    ], []);

  const grouped = checkpoints
    .reduce((a, { key, value }) => ({
      ...a,
      [key]: [...(a[key] || []), value],
    }), {});

  const stats = Object.entries(grouped)
    .reduce((a, [key, values]) => ({
      ...a,
      [key]: calculateStats(values),
    }), {});

  const pipelineUtilization = Object.entries(stats)
    .filter(([key]) => key.endsWith('stream.pipeline.time'))
    .reduce((a, [key, { count }]) => ({
      ...a,
      [`${splitKey(key).pipeline}|stream.pipeline.utilization`]: count / collected.length,
    }), {});

  return { ...functionMetrics, ...pipelineUtilization, ...stats };
};

export const toPromiseWithMetrics = (s) =>
  new Promise((resolve, reject) => {
    clear();
    const collected = [];
    s.consume((err, x, push, next) => {
      if (err) {
        reject(err);
      } else if (x === _.nil) {
        const metrics = calculateMetrics(collected);
        // logMetrics(metrics);
        resolve(metrics);
      } else {
        collected.push(x);
        next();
      }
    })
      .resume();
  });
