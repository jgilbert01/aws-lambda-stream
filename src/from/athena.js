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
