import _ from 'highland';
import {
  faulty,
  faults,
  flushFaults,
  fromKinesis,
  fromSqsEvent,
  toPromise,
  defaultOptions,
} from 'aws-lambda-stream';

const { debug } = defaultOptions;

export class Handler {
  handle(event) {
    // return fromKinesis(event)
    return fromSqsEvent(event)
    // .tap(uow => console.log(uow.event))

    // filter old - 2 weeks

      .map(toEmbeddedMetric)
      .map(flushMetrics)

      .errors(faults)
      .through(flushFaults(defaultOptions));
  }
}

export const handle = async (event, context) => {
  debug('event: %j', event);
  debug('context: %j', context);

  return new Handler()
    .handle(event)
    // .tap(debug)
    .through(toPromise);
};

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html

const Dimensions = [[
  'account',
  'region',
  // 'stream',
  // 'shard',
  'stage',
  'source',
  'functionname',
  'pipeline',
  'type',
]];

const Metrics = [
  {
    Name: 'domain.event',
    Unit: 'Count',
  },
  {
    Name: 'domain.event.size',
    Unit: 'Bytes',
  },
];

const toEmbeddedMetric = faulty((uow) => {
  const Namespace = process.env.NAMESPACE;
  const Timestamp = uow.event.timestamp;

  return {
    ...uow,
    embeddedMetrics: {
      '_aws': {
        Timestamp,
        CloudWatchMetrics: [
          {
            Namespace,
            Dimensions,
            Metrics,
          },
        ],
      },
      'account': (uow.event.tags && uow.event.tags.account) || process.env.ACCOUNT_NAME || 'not-specified',
      'region': uow.record.awsRegion || /* istanbul ignore next */ (uow.event.tags && uow.event.tags.region) || /* istanbul ignore next */ process.env.AWS_REGION,
      // 'stream': (uow.record.eventSourceARN && /* istanbul ignore next */ uow.record.eventSourceARN.split('/')[1]) || 'not-specified',
      // 'shard': uow.record.eventID.split('-')[1].split(':')[0],
      'stage': (uow.event.tags && uow.event.tags.stage) || 'not-specified',
      'source': (uow.event.tags && uow.event.tags.source) || 'not-specified',
      'functionname': (uow.event.tags && uow.event.tags.functionname) || 'not-specified',
      'pipeline': (uow.event.tags && uow.event.tags.pipeline) || 'not-specified',
      'type': uow.event.type,
      'domain.event': 1,
      'domain.event.size': JSON.stringify(uow.event).length,
    },
  };
});

const flushMetrics = faulty((uow) => {
  console.log(JSON.stringify(uow.embeddedMetrics));
  return uow;
});
