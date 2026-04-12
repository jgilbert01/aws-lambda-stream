import { fromSqs } from './sqs';
import { faulty } from '../utils';

/*

here is an example of the yaml to route athena and scheduler events
to a queue and listener function using this fromAthena function

resources:
  Resources:
    EbListenerQueue:
      Type: AWS::SQS::Queue
      Properties:
        QueueName: ${self:service}-${opt:stage}-listener-eb

    EbListenerQueuePolicy:
      Type: AWS::SQS::QueuePolicy
      Properties:
        Queues:
          - Ref: EbListenerQueue
        PolicyDocument:
          Statement:
            - Effect: Allow
              Principal:
                Service: events.amazonaws.com
              Action: sqs:SendMessage
              Resource:
                Fn::GetAtt: [ EbListenerQueue, Arn ]
              Condition:
                ArnEquals:
                  aws:SourceArn:
                    Fn::GetAtt: [ AthenaEventRule, Arn ]
            - Effect: Allow
              Principal:
                Service: events.amazonaws.com
              Action: sqs:SendMessage
              Resource:
                Fn::GetAtt: [ EbListenerQueue, Arn ]
              Condition:
                ArnEquals:
                  aws:SourceArn:
                    Fn::GetAtt: [ SchedulerEventRule, Arn ]

    AthenaEventRule: 
      Type: AWS::Events::Rule
      Properties: 
        EventBusName: default
        EventPattern:
          source: 
            - aws.athena
          detail:
            workgroupName:
              - ${self:provider.environment.WORK_GROUP}
        State: ENABLED
        Targets: 
          - Id: Channel
            Arn: 
              Fn::GetAtt: [ EbListenerQueue, Arn ]

    SchedulerEventRule: 
      Type: AWS::Events::Rule
      Properties: 
        EventBusName: ${cf:${self:custom.sys}-${self:custom.subsys}-event-hub-${opt:stage}.busName}
        EventPattern:
          source: 
            - ${self:service}-${opt:stage}
        State: ENABLED
        Targets: 
          - Id: Channel
            Arn: 
              Fn::GetAtt: [ EbListenerQueue, Arn ]

  Outputs:
    EbListenerQueue:
      Value:
        Ref: EbListenerQueue
*/

// https://docs.aws.amazon.com/athena/latest/ug/athena-events.html
const TYPE_MAP = {
  QUEUED: 'athena-query-queued',
  RUNNING: 'athena-query-running',
  SUCCEEDED: 'athena-query-succeeded',
  FAILED: 'athena-query-failed',
};

const toEventEnvelope = (uow) => ({
  ...uow,
  event: {
    ...uow.event,
    type: TYPE_MAP[uow.event.detail.currentState] || uow.event.detail.type,
    timestamp: (new Date(uow.event.time)).getTime(),
    partitionKey: uow.event.detail?.queryExecutionId || uow.event.detail?.pk,
  },
});

export const fromAthena = (event, options = {}) => fromSqs(event)
  // .tap(console.log)
  .map((uow) => ({
    ...uow,
    event: JSON.parse(uow.record.body),
  }))
  .map(faulty(toEventEnvelope));

// // uow.record.s3.s3.object.key.match(opt.pk || /table=(\d+)\/year=(\d+)\/month=(\d+)\/day=(\d+)\/hour=(\d+)\/minute=(\d+)/),

// export const decodeKey = (key) => decodeURIComponent(key)
//   .split('/')
//   .reduce((a, c, i, r) => {
//     if (c.includes('=')) {
//       const [k, v] = c.split('=');
//       return {
//         ...a,
//         [k]: v,
//       };
//     } else if (c.includes('.')) {
//       const [k, v] = c.split('.');
//       return {
//         ...a,
//         file: k,
//         contentType: v, // TODO .metadata ???
//       };
//     } else if (r[r.length - 1] === c) {
//       return {
//         ...a,
//         file: c,
//       };
//     } else {
//       return {
//         ...a,
//         prefix: [...a.prefix, c],
//       };
//     }
//   }, {
//     prefix: [],
//   });

// export const buildS3PartitionKey = (key, level = 1) => key.split('/').slice(0, -level).join('/');

// export const mapPartition = (uow) => ({
//   event: {
//     id: uow.record.sqs.messageId, // `${uow.record.s3.s3.object.key}-${uow.record.s3.s3.object.eTag}`, // sequencer
//     type: uow.record.s3.eventName,
//     timestamp: (new Date(uow.record.s3.eventTime)).getTime(),
//     partitionKey: buildS3PartitionKey(decodeURIComponent(uow.record.s3.s3.object.key)),
//     //   tags: {
//     //     // account
//     //     // region
//     //   },
//     notification: {
//       type: uow.record.s3.eventName,
//       bucket: uow.record.s3.s3.bucket.name,
//       key: decodeURIComponent(uow.record.s3.s3.object.key),
//       partition: decodeKey(uow.record.s3.s3.object.key),
//     },
//     raw: uow.record.s3,
//   },
// });

// export const mapJobRequest = (uow) => ({
//   getJobRequest: {
//     Bucket: uow.record.s3.s3.bucket.name,
//     Key: uow.record.s3.s3.object.key,
//     VersionId: uow.record.s3.s3.object.versionId,
//   },
// });

// export const fromS3 = (event, options = {}) => fromSqsSnsS3(event)
//   .map((uow) => ({
//     ...uow,
//     isJob: uow.record.s3.s3.object.key.includes('/jobs/'),
//   }))
//   .map((uow) => ({
//     ...uow,
//     ...(!uow.isJob && mapPartition(uow)),
//     ...(uow.isJob && mapJobRequest(uow)),
//   }))
//   .through(getObjectFromS3({
//     id: 'handler:fromS3',
//     getRequestField: 'getJobRequest',
//     getResponseField: 'getJobResponse',
//     additionalClientOpts: {
//       followRegionRedirects: true,
//     },
//     ...options,
//   }))
//   .map(faulty((uow) => {
//     if (!uow.getJobResponse) return uow;

//     const job = JSON.parse(Buffer.from(uow.getJobResponse.Body));
//     return ({
//       ...uow,
//       event: {
//         id: uow.record.sqs.messageId,
//         type: job.type,
//         timestamp: (new Date(uow.record.s3.eventTime)).getTime(),
//         job: {
//           id: uow.record.s3.s3.object.versionId,
//           ...job,
//         },
//         raw: uow.event,
//       },
//     });
//   }));
