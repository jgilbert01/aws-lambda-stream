// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html
import debug from 'debug';
import Timer from './timer';
import { envTags } from '../utils/tags';

const log = debug('handler:metrics');

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
const formatUnit = ({ metric, key }) => {
  if (metric.endsWith('count') || key === 'count') {
    return 'Count';
  }
  if (metric.endsWith('bytes')) {
    return 'Bytes';
  }
  if (metric.endsWith('time')) {
    return 'Milliseconds';
  }
  if (metric.endsWith('utilization')) {
    return 'Percent';
  }
  return 'None';
};

const formatValue = ({
  metric, value,
}) => {
  if (typeof value === 'object') {
    return Object.entries(value).map(([key, v]) => ({
      Name: `${metric}.${key}`,
      Unit: formatUnit({ metric, key }),
      value: v,
    }));
  } else {
    return [{
      Name: metric,
      Unit: formatUnit({ metric }),
      value,
    }];
  }
};

const formatEntry = ({
  Namespace, Metrics, Dimensions, Timestamp, tags, values,
}) =>
  ({
    ...tags,
    ...values,
    _aws: {
      Timestamp,
      CloudWatchMetrics: [
        {
          Namespace,
          Dimensions,
          Metrics,
        },
      ],
    },
  });

const FunctionDimensions = [
  'functionname',
  'source',
  'stage',
  'region',
  'account',
];

const PipelineDimensions = [
  'pipeline',
  ...FunctionDimensions,
];

const StepDimensions = [
  'step',
  ...PipelineDimensions,
];

export const formatMetrics = (metrics) => {
  const Timestamp = Timer.now();
  const Namespace = process.env.NAMESPACE;
  const tags = envTags();
  delete tags.pipeline;

  const levels = Object.entries(metrics)
    .reduce((a, [key, value]) => {
      const [k1, k2, k3] = key.split('|');
      const step = k3 ? k2 : undefined;
      const pipeline = (!k3 && !k2) ? undefined : k1;
      const metric = k3 || k2 || k1;

      if (step) {
        const k = `${pipeline}|${step}`;
        return {
          ...a,
          step: {
            ...a.step,
            [k]: [...(a.step[k] || []), ...formatValue({
              metric, value,
            })],
          },
        };
      } else if (pipeline) {
        return {
          ...a,
          pipeline: {
            ...a.pipeline,
            [pipeline]: [...(a.pipeline[pipeline] || []), ...formatValue({
              metric, value,
            })],
          },
        };
      } else {
        return {
          ...a,
          function: [...a.function, ...formatValue({ metric, value })],
        };
      }
    }, { function: [], pipeline: [], step: [] });

  // console.log('levels: ', JSON.stringify(levels, null, 2));

  return [
    // function level
    formatEntry({
      Namespace,
      Dimensions: FunctionDimensions,
      Timestamp,
      Metrics: levels.function.map(({ Name, Unit }) => ({ Name, Unit })),
      tags,
      values: levels.function.reduce((a, { Name, value }) => ({ ...a, [Name]: value }), {}),
    }),

    // pipeline level
    ...Object.entries(levels.pipeline)
      .map(([pipeline, values]) => formatEntry({
        Namespace,
        Dimensions: PipelineDimensions,
        Timestamp,
        Metrics: values.map(({ Name, Unit }) => ({ Name, Unit })),
        tags: {
          ...tags,
          pipeline,
        },
        values: values.reduce((a, { Name, value }) => ({ ...a, [Name]: value }), {}),
      })),

    // step level
    ...Object.entries(levels.step)
      .map(([key, values]) => formatEntry({
        Namespace,
        Dimensions: StepDimensions,
        Timestamp,
        Metrics: values.map(({ Name, Unit }) => ({ Name, Unit })),
        tags: {
          ...tags,
          ...(([pipeline, step]) => ({ pipeline, step }))(key.split('|')),
        },
        values: values.reduce((a, { Name, value }) => ({ ...a, [Name]: value }), {}),
      })),
  ];
};

export const logMetrics = (metrics) => {
  /* istanbul ignore if */
  if (process.env.ENABLE_EMF === 'true') {
    const emf = formatMetrics(metrics);
    emf.forEach((m) => {
      console.log(JSON.stringify(m, null, 2));
    });
  } else {
    log('%j', metrics);
  }
};
