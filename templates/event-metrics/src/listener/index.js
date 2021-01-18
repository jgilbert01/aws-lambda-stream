import _ from 'highland';
import {
  fromKinesis,
  toPromise,
  printStart,
  printEnd,
  debug as d,
} from 'aws-lambda-stream';

import Connector from '../connectors/cloudwatch';

const debug = d('handler');

export class Handler {
  handle(event) {
    return fromKinesis(event)
      .tap(printStart)

      // TODO group
      // TODO batch 20 / count & size
      .map(map)

      .map(put(debug))
      .parallel(Number(process.env.PARALLEL) || 4)

      .tap(printEnd);
  }
}

export const handle = async (event, context) => {
  debug('event: %j', event);
  debug('context: %j', context);

  return new Handler()
    .handle(event)
    .tap(debug)
    .through(toPromise);
};


const map = (uow) => {
  const Timestamp = Number(uow.event.timestamp.toString().substring(0, 10));
  const Dimensions = [
    {
      Name: 'account',
      Value: (uow.event.tags && uow.event.tags.account) || process.env.ACCOUNT_NAME || 'not-specified',
    }, {
      Name: 'region',
      Value: uow.record.awsRegion || /* istanbul ignore next */ (uow.event.tags && uow.event.tags.region) || /* istanbul ignore next */ process.env.AWS_REGION,
      // }, {
      //   Name: 'stream',
      //   Value: uow.record.eventSourceARN.split('/')[1],
    }, {
      Name: 'shard',
      Value: uow.record.eventID.split('-')[1].split(':')[0],
    }, {
      Name: 'stage',
      Value: (uow.event.tags && uow.event.tags.stage) || 'not-specified',
    }, {
      Name: 'source',
      Value: (uow.event.tags && uow.event.tags.source) || 'not-specified',
    }, {
      Name: 'functionname',
      Value: (uow.event.tags && uow.event.tags.functionname) || 'not-specified',
    }, {
      Name: 'pipeline',
      Value: (uow.event.tags && uow.event.tags.pipeline) || 'not-specified',
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
        Value: uow.record.kinesis.data.length,
        Dimensions,
      },
    ],
  };
};

const put = _debug => (uow) => {
  const p = new Connector(_debug)
    .put(uow.Namespace, uow.MetricData)
    .then(response => ({
      ...uow,
      response,
    }));

  return _(p);
};
