import { putMetrics } from 'aws-lambda-stream';

const pipeline = options => s => s
  .tap(uow => console.log(uow.event))

  // TODO switch to embedded metrics
  // https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html
  .map(toPutRequest)
  .through(putMetrics(options));

const toPutRequest = (uow) => {
  const Timestamp = Number(uow.event.timestamp.toString().substring(0, 10));
  const Dimensions = [
    {
      Name: 'account',
      Value: (uow.event.tags && uow.event.tags.account) || /* istanbul ignore next */ process.env.ACCOUNT_NAME || /* istanbul ignore next */ 'not-specified',
    }, {
      Name: 'region',
      Value: uow.record.awsRegion || /* istanbul ignore next */ (uow.event.tags && uow.event.tags.region) || /* istanbul ignore next */ process.env.AWS_REGION,
    }, {
      Name: 'stage',
      Value: (uow.event.tags && uow.event.tags.stage) || 'not-specified',
    }, {
      Name: 'source',
      Value: (uow.event.tags && uow.event.tags.source) || 'not-specified',
    }, {
      Name: 'functionname',
      Value: (uow.event.tags && uow.event.tags.functionname) || /* istanbul ignore next */ 'not-specified',
    }, {
      Name: 'pipeline',
      Value: (uow.event.tags && uow.event.tags.pipeline) || /* istanbul ignore next */ 'not-specified',
    }, {
      Name: 'type',
      Value: uow.event.type,
    },
  ];

  return {
    ...uow,
    Namespace: process.env.NAMESPACE,
    MetricData: [
      {
        MetricName: 'domain.event',
        Timestamp,
        Unit: 'Count',
        Value: 1,
        Dimensions,
      }, {
        MetricName: 'domain.event.size',
        Timestamp,
        Unit: 'Bytes',
        Value: JSON.stringify(uow.event).length,
        Dimensions,
      },
    ],
  };
};

export default pipeline;
