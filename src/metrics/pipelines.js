import _ from 'highland';
import Promise from 'bluebird';

import Timer from './timer';

let funcMetrics = {};
export const functionMetrics = (m) => {
  /* istanbul ignore next */
  if (m) funcMetrics = m;
  return funcMetrics;
};

export const clear = (opt) => {
  funcMetrics = {};
  if (opt.xrayEnabled) {
    require('./xray').clearPipelineSegments();
  }
};

export const startUow = (publishTime, batchSize) => {
  funcMetrics['stream.batch.utilization'] = batchSize / Number(process.env.BATCH_SIZE || /* istanbul ignore next */ 100);

  return new PipelineMetrics({
    publishTime,
  });
};

export const adornKinesisMetrics = (uow, event) => {
  const time = `${uow.record.kinesis.approximateArrivalTimestamp}`;
  uow.metrics = startUow(Number(`${time.substring(0, 10)}${time.substring(11)}`), event.Records.length);
};

/* istanbul ignore next */
export const adornDynamoMetrics = (uow, event) => {
  uow.metrics = startUow(uow.record.dynamodb.ApproximateCreationDateTime * 1000, event.Records.length);
};

/* istanbul ignore next */
export const adornSqsMetrics = (uow, event) => {
  uow.metrics = startUow(uow.record.attributes.SentTimestamp, event.Records.length);
};

class PipelineMetrics {
  constructor({
    pipeline, publishTime, timer, gauges, opt,
  }) {
    this.pipeline = pipeline || 'default';
    this.timer = new Timer({
      start: publishTime || timer?.start,
      last: timer?.last,
      checkpoints: timer?.checkpoints,
    });
    this.gauges = gauges || {};
    this.opt = opt;
  }

  gauge(key, value) {
    // console.log('gauge: ', key, value);
    const k = `${this.pipeline}|${key}`;
    this.gauges[k] = [...(this.gauges[k] || []), ...(Array.isArray(value) ? value : [value])];
    return this;
  }

  startPipeline({ pipeline }, opt) {
    const clone = new PipelineMetrics({
      pipeline,
      timer: this.timer,
      opt,
    });

    // time waiting on channel capacity (e.g. shard count)
    clone.timer.checkpoint(`${pipeline}|stream.channel.wait.time`);

    // Initialize an xray segment if enabled
    if (opt.xrayEnabled) {
      clone.xraySegment = require('./xray').startPipelineSegment(pipeline);
    }

    return clone;
  }

  endPipeline() {
    this.timer.end(`${this.pipeline}|stream.pipeline.time`);
    return this;
  }

  startStep(step) {
    /* istanbul ignore else */
    if (this.opt?.metrics.enabled('step')) {
      // time waiting for io capacity (e.g parallel count)
      this.timer.checkpoint(`${this.pipeline}|${step}|stream.pipeline.io.wait.time`);
    }
    return this;
  }

  endStep(step) {
    /* istanbul ignore else */
    if (this.opt?.metrics.enabled('step')) {
      this.timer.checkpoint(`${this.pipeline}|${step}|stream.pipeline.io.time`);
    }
    return this;
  }

  // wrap promise
  w(p, step) {
    const self = this;
    self.startStep(step);
    return Promise.resolve(p()) // now start the promise
      .tap(() => self.endStep(step));
  }
}

export const endPipeline = (pipelineId, opt, s) =>
  s.consume((err, x, push, next) => {
    /* istanbul ignore if */
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      if (opt.xrayEnabled) {
        require('./xray').terminateSegment(pipelineId);
      }
      push(null, x);
    } else {
      // per uow
      x.metrics?.endPipeline();
      push(null, x);
      next();
    }
  });
