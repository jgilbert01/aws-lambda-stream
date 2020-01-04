/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import * as aws from 'aws-sdk';

import _ from 'highland';
import merge from 'lodash/merge';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const ttl = (start, days) => (start / 1000) + (60 * 60 * 24 * days);

export const updateExpression = (Item) => ({
  ExpressionAttributeNames: Object.keys(Item)
    .map((attrName) => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {}),
  ExpressionAttributeValues: Object.keys(Item)
    .map((attrName) => ({ [`:${attrName}`]: Item[attrName] }))
    .reduce(merge, {}),
  UpdateExpression: `SET ${Object.keys(Item)
    .map((attrName) => `#${attrName} = :${attrName}`)
    .join(', ')}`,
  ReturnValues: 'ALL_NEW',
});

export const timestampCondition = (fieldName = 'timestamp') => ({
  ConditionExpression: `attribute_not_exists(#${fieldName}) OR #${fieldName} < :${fieldName}`,
});

export const update = ({
  debug = d('dynamodb'),
  tableName = process.env.ENTITY_TABLE_NAME,
  updateRequestField = 'updateRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
} = {}) => {
  const connector = new Connector({ debug, tableName });

  const invoke = (uow) => {
    const p = connector.update(uow[updateRequestField])
      .then((updateResponse) => ({ ...uow, updateResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

// testing
export const toDynamodbRecords = (events) => ({
  Records: events.map((e, i) =>
    ({
      eventID: `${i}`,
      eventName: !e.oldImage ? 'INSERT' : !e.newImage ? 'REMOVE' : 'MODIFY', // eslint-disable-line no-nested-ternary
      // eventVersion: '1.0',
      eventSource: 'aws:dynamodb',
      awsRegion: 'us-west-2',
      dynamodb: {
        ApproximateCreationDateTime: e.timestamp,
        Keys: e.keys ? aws.DynamoDB.Converter.marshall(e.keys) : /* istanbul ignore next */ undefined,
        NewImage: e.newImage ? aws.DynamoDB.Converter.marshall(e.newImage) : undefined,
        OldImage: e.oldImage ? aws.DynamoDB.Converter.marshall(e.oldImage) : undefined,

        SequenceNumber: `${i}`,
        // SizeBytes: 59,
        StreamViewType: 'NEW_AND_OLD_IMAGES',
      },
      // eventSourceARN: 'arn:aws:dynamodb:us-west-2:123456789012:table/myservice-entities/stream/2016-11-16T20:42:48.104',
    })),
});
