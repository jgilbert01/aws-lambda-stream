/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import * as aws from 'aws-sdk';
import _ from 'highland';

import { faulty } from '../utils';

export const fromDynamodb = (event) => // eslint-disable-line import/prefer-default-export

  // prepare the event stream
  _(event.Records)

    .map(faulty((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: {
          id: record.eventID,
          type: `${calculateEventTypePrefix(record)}-${calculateEventTypeSuffix(record)}`,
          partitionKey: record.dynamodb.Keys.hk.S,
          timestamp: record.dynamodb.ApproximateCreationDateTime * 1000,
          tags: {
            region: record.awsRegion,
          },
          raw: {
            new: record.dynamodb.NewImage
              ? aws.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage)
              : undefined,
            old: record.dynamodb.OldImage
              ? aws.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage)
              : undefined,
          },
        },
      })));

// https://www.trek10.com/blog/dynamodb-single-table-relational-modeling
// all rows must have a discriminator field to store the prefix: <entityname> or <entityname-relationship>
// for node rows this could be the same value as the sk
// for edge rows the sk is used as the fk value
// so if the table includes edge rows then the discriminator field should hold the prefix
const calculateEventTypePrefix = (record) => {
  const image = record.dynamodb.NewImage || record.dynamodb.OldImage;
  const discriminator = image.discriminator || image.sk;
  return discriminator.S.toLowerCase();
};

const calculateEventTypeSuffix = (record) => (
  {
    INSERT: 'created',
    MODIFY: 'updated',
    REMOVE: 'deleted',
  }[record.eventName]
);
