import _ from 'highland';
/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { addMinutes } from 'date-fns';
import {
  printStartPipeline,
  printEndPipeline,
  faulty,
  ttl,
  compact,
} from '../utils';
import {
  filterOnEventType,
  filterOnContent,
} from '../filters';
import {
  updateDynamoDB,
  updateExpression,
  timestampCondition,
} from '../sinks/dynamodb';
import {
  queryAllDynamoDB,
  toPkQueryRequest,
} from '../queries/dynamodb';
import { startQueryExecution } from '../sinks/athena';

// -------------------------
// used with update flavor
// to create query entity
// -------------------------

export const toQueryUpdateRequest = faulty((uow, rule) => ({
  Key: {
    // Note MUST use underscore to support scheduler name restrictions: [0-9a-zA-Z-_.]+
    pk: `${rule.id}${rule.correlationKey ? /* istanbul ignore next */ `_${rule.correlationKey(uow, rule)}` : ''}`,
    sk: uow.event.id, // original event id
  },
  ...updateExpression({
    queryId: rule.id,
    directive: rule.delay ? /* istanbul ignore next */ 'SCHEDULE' : 'IMMEDIATE',
    delay: rule.delay,

    notification: uow.event.notification,
    original: uow.event.event, // TODO ???
    // partition TODO ???

    discriminator: 'query',
    timestamp: uow.event.timestamp,
    ttl: ttl(uow.event.timestamp, 33),
    awsregion: process.env.AWS_REGION,
  }),
  ...timestampCondition(),
}));

// -------------------------
// schedule queries
// for kickoff, retries and prioritization
// -------------------------

export const toScheduleRequest = faulty((uow) => {
  const date = new Date(uow.event.timestamp);
  const schedule = addMinutes(date, uow.event.raw.new.delay);
  const Source = `${process.env.PROJECT}-${process.env.STAGE}`;
  const Description = `${Source}_${uow.event.raw.new.pk}`;
  const eventType = 'athena-query-schedule-expired';

  return ({
    Name: Description.substring(Description.length - 64),
    Description,
    ClientToken: uow.event.id,
    ScheduleExpression: `at(${schedule.toISOString().substring(0, 19)})`,
    Target: {
      EventBridgeParameters: {
        DetailType: eventType,
        Source,
      },
      Input: JSON.stringify({
        type: eventType,
        pk: uow.event.raw.new.pk,
        sk: uow.event.raw.new.sk,
      }),
      Arn: process.env.BUS_ARN,
      RoleArn: process.env.SCHEDULER_ROLE_ARN,
    },

    ActionAfterCompletion: 'DELETE',
    FlexibleTimeWindow: {
      Mode: 'OFF',
    },
    KmsKeyArn: process.env.REGIONAL_MASTER_KEY_ARN,
  });
});

export const toQueryImmediateUpdateRequest = faulty((uow) => ({
  Key: { // from schedule expired event
    pk: uow.event.detail.pk,
    sk: uow.event.detail.sk,
  },
  ...updateExpression({
    directive: 'IMMEDIATE',
    timestamp: uow.event.timestamp,
    awsregion: process.env.AWS_REGION,
  }),
  ...timestampCondition(),
}));

// -------------------------
// start query execution
// trigger by discriminator = query
// -------------------------

export const executeQuery = (rule) => (s) => s
  .map(normalize)
  // .tap((uow) => rule.debug('%j', { uow }))
  .filter(onEventType(rule))
  .filter(onContent(rule))
  .tap(printStartPipeline)

  .through(compact(rule))
  .through(guard(rule))

  .map(toOutputLocationRequest(rule))
  .map(toQueryRequest(rule))
  .through(startQueryExecution(rule))

  .map(toQueryResponseUpdateRequest({ rate: { num: 1, ms: 2000 }, ...rule }))
  .through(updateDynamoDB(rule))

  .tap(printEndPipeline);

const onEventType = (rule) => faulty((uow) => filterOnEventType(rule, uow));
const onContent = (rule) => faulty((uow) => filterOnContent(rule, uow));

export const normalize = (uow) => ({
  ...uow,
  event: {
    ...uow.event,
    // make stored query entity similar to original s3 event
    ...uow?.event?.raw?.new,
    raw: undefined,
  },
  raw: uow?.event?.raw,
});

const toOutputLocationRequest = (rule) => faulty((uow) => ({
  ...uow,
  outputLocationRequest: !uow.guard || /* istanbul ignore next */ uow.guard.proceed
    ? rule.toOutputLocation
      ? rule.toOutputLocation(uow, rule)
      : /* istanbul ignore next */ `${process.env.ATHENA_OUTPUT_LOCATION}`
    : /* istanbul ignore next */ undefined,
}));

const toQueryRequest = (rule) => faulty((uow) => ({
  ...uow,
  queryRequest: !uow.guard || /* istanbul ignore next */ uow.guard.proceed
    ? rule.toQueryRequest
      ? rule.toQueryRequest(uow, rule)
      : /* istanbul ignore next */ undefined
    : /* istanbul ignore next */ undefined,
}));

const toQueryResponseUpdateRequest = ({
  queryResponseField = 'queryResponse',
  ...rule
}) => faulty((uow) => ({
  ...uow,
  updateRequest: {
    TableName: process.env.ENTITY_TABLE_NAME,
    Key: {
      pk: uow.record.dynamodb.Keys.pk.S,
      sk: uow.record.dynamodb.Keys.sk.S,
    },
    ...updateExpression({
      // set exec id
      data: uow[queryResponseField]?.QueryExecutionId,

      // delay execution so more data can come in
      ...(uow.guard?.delay && /* istanbul ignore next */ {
        directive: 'SCHEDULE',
        delay: uow.guard.delay,
        disableGuard: true,
        timestamp: Date.now(),
      }),

      // setup retry
      ...(uow[queryResponseField]?.retry && /* istanbul ignore next */ {
        // retry via scheduler
        directive: 'SCHEDULE',
        delay: 5, // minutes
        timestamp: Date.now(),
        retry: {
          eventID: uow.record.eventID,
          timestamp: uow.record.dynamodb.ApproximateCreationDateTime,
          pipeline: uow.pipeline,
          // TODO add counter ???
        },
      }),
      awsregion: process.env.AWS_REGION,
    }),
  },
}));

// -------------------------
// guard query execution
// until enough data/tables are ready
// -------------------------

export const guard = (rule) => {
  /* istanbul ignore else */
  if (!rule.guard) {
    return (s) => s;
  } else {
    return (s) => s
      .map(toCorrelatedTablesQueryRequest(rule))
      .through(queryAllDynamoDB({
        ...rule,
        queryRequestField: 'correlatedTablesQueryRequest',
        queryResponseField: 'correlatedTables',
      }))

      .map(checkGuard(rule));
  }
};

/* istanbul ignore next */
const toCorrelatedTablesQueryRequest = (rule) => (uow) => ({
  ...uow,
  correlatedTablesQueryRequest: toPkQueryRequest(uow, rule),
});

export const checkGuard = (rule) => faulty((uow) => {
  /* istanbul ignore else */
  if (uow.event?.disableGuard) {
    return {
      ...uow,
      guard: {
        proceed: true,
      },
    };
  }

  /* istanbul ignore next */
  return ({
    ...uow,
    guard: rule.guard(uow, rule),
  });
});

// -------------------------
// track query execution with update flavor
// -------------------------

export const toLinkedQueryRequest = faulty((uow, rule) => ({
  IndexName: rule.indexNm || 'gsi2',
  KeyConditionExpression: '#pk = :pk',
  ExpressionAttributeNames: {
    '#pk': rule.indexFn || 'data',
  },
  ExpressionAttributeValues: {
    ':pk': uow.event.partitionKey,
  },
  ConsistentRead: false,
}));

const parseLinkedQuery = (uow) => {
  // console.log({ uow });
  const records = uow.queryResponse.filter((r) => r.discriminator === 'query');

  /* istanbul ignore if */
  if (records.length !== 1) {
    throw new Error('No Linked Query Found');
  }

  return records[0];
};

export const toSucceededUpdateRequest = faulty((uow) => {
  const linkedQuery = parseLinkedQuery(uow);
  return {
    Key: {
      pk: linkedQuery.pk,
      sk: linkedQuery.sk,
    },
    ...updateExpression({
      // use this update/directive to trigger next set of queries
      directive: 'SUCCEEDED',
      awsregion: process.env.AWS_REGION,
    }),
    ...timestampCondition(),
  };
});

export const toRetryUpdateRequestOrFault = faulty((uow) => {
  const linkedQuery = parseLinkedQuery(uow);
  if (uow.event.detail.athenaError?.retryable) {
    return {
      Key: {
        pk: linkedQuery.pk,
        sk: linkedQuery.sk,
      },
      ...updateExpression({
        // retry via scheduler
        directive: 'SCHEDULE',
        delay: 5,
        retry: {
          eventID: uow.event.id,
          timestamp: uow.event.timestamp,
          pipeline: linkedQuery.queryId,
          // TODO count
        },
        awsregion: process.env.AWS_REGION,
      }),
      ...timestampCondition(),
    };
  } else {
    uow.pipeline = linkedQuery.queryId || /* istanbul ignore next */ uow.pipeline;
    throw new Error(uow.event.detail.athenaError.errorMessage);
  }
});
