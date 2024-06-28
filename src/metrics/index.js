import _ from 'highland';
import Promise from 'bluebird';

import Timer from './timer';
// import { addMiddleware } from './middleware';

let functionMetrics = {};

export const clear = () => {
  functionMetrics = {};
  // TODO conditional xray
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
    pipeline, publishTime, timer, gauges,
  }) {
    this.pipeline = pipeline || 'default';
    this.timer = new Timer({
      start: publishTime || timer?.start,
      last: timer?.last,
      checkpoints: timer?.checkpoints,
    });
    this.gauges = gauges || {};
  }

  gauge(key, value) {
    // console.log('gauge: ', key, value);
    const k = `${this.pipeline}|${key}`;
    this.gauges[k] = [...(this.gauges[k] || []), ...(Array.isArray(value) ? value : [value])];
    return this;
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

    // TODO conditional xray
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

export const endPipeline = (s, pipelineId) =>
  s.consume((err, x, push, next) => {
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      // TODO conditional xray
      push(null, x);
    } else {
      // per uow
      x.metrics?.endPipeline();
      push(null, x);
      next();
    }
  });

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
    ], [])
    .reduce((a, { key, value }) => ({
      ...a,
      [key]: [...(a[key] || []), value],
    }), {});

  // console.log('checkpoints: ', checkpoints);

  const gauges = collected
    .reduce((a, { metrics: { gauges } }) => [ // eslint-disable-line no-shadow
      ...a,
      ...Object.entries(gauges)
        .map(([key, value]) => ({ key, value })),
    ], [])
    .reduce((a, { key, value }) => ({
      ...a,
      [key]: [...(a[key] || []), ...value],
    }), {});

  // console.log('gauges: ', gauges);

  const stats = Object.entries({
    ...checkpoints,
    ...gauges,
  })
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

  return {
    ...functionMetrics,
    ...pipelineUtilization,
    ...stats,
  };
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

export const capture = (client, command, step, opt, ctx) =>
  // console.log('capture: ', step, opt, ctx);
  // addMiddleware(client, command, step, opt, ctx);
  // TODO conditional xray
  client || command;
