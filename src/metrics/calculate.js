import { functionMetrics } from './pipelines';

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

export const calculateMetrics = (collected) => { // eslint-disable-line import/prefer-default-export
  // console.log('collected: ', JSON.stringify(collected.map((u) => u.metrics), null, 2));

  functionMetrics()['stream.uow.count'] = collected.length;

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
    ...functionMetrics(),
    ...pipelineUtilization,
    ...stats,
  };
};
