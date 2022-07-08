/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import * as aws from 'aws-sdk';
import _ from 'highland';

import { faulty } from '../utils';

export const fromDynamodb = (event, {
  pkFn = 'pk',
  skFn = 'sk',
  discriminatorFn = 'discriminator',
  eventTypePrefix = undefined,
} = {}) => // eslint-disable-line import/prefer-default-export

  // prepare the event stream
  _(event.Records)

    //--------------------------------
    // global table support
    .filter(outReplicas)
    .filter(outGlobalTableExtraModify)
    //--------------------------------

    .map(faulty((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
        event: {
          id: record.eventID,
          type: `${calculateEventTypePrefix(record, { skFn, discriminatorFn, eventTypePrefix })}-${calculateEventTypeSuffix(record)}`,
          partitionKey: record.dynamodb.Keys[pkFn].S,
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
const calculateEventTypePrefix = (record, opt) => {
  /* istanbul ignore if */ if (opt.eventTypePrefix) return opt.eventTypePrefix;

  const image = record.dynamodb.NewImage || record.dynamodb.OldImage;
  const discriminator = image[opt.discriminatorFn] || image[opt.skFn];
  return discriminator.S.toLowerCase();
};

const calculateEventTypeSuffix = (record) => {
  const suffix = ({
    INSERT: 'created',
    MODIFY: 'updated',
    REMOVE: 'deleted',
  }[record.eventName]);

  if (suffix !== 'deleted') {
    const { NewImage, OldImage } = record.dynamodb;

    if ((NewImage && NewImage.deleted) || (OldImage && OldImage.deleted)) {
      if (NewImage && NewImage.deleted && NewImage.deleted.BOOL) {
        return 'deleted';
      } else {
        /* istanbul ignore else */
        if (OldImage && OldImage.deleted && OldImage.deleted.BOOL) { // eslint-disable-line no-lonely-if
          return 'undeleted';
        }
      }
    }
  }

  return suffix;
};

//--------------------------------------------
// global table support - version: 2017.11.29
//--------------------------------------------

export const outReplicas = (record) => {
  const image = record.dynamodb.NewImage || record.dynamodb.OldImage;

  // is this a global table event
  // v1
  /* istanbul ignore next */
  if (image['aws:rep:updateregion']) {
    // only process events from the current region
    return image['aws:rep:updateregion'].S === process.env.AWS_REGION;
  }

  // v2
  if (image.awsregion) {
    // only process events from the current region
    return image.awsregion.S === process.env.AWS_REGION;
  }

  return true;
};

// dynamodb stream emits extra events as it adorns the 'aws:rep' global table metadata
export const outGlobalTableExtraModify = (record) => {
  const { NewImage, OldImage } = record.dynamodb;

  // v1
  /* istanbul ignore next */
  if (NewImage && NewImage['aws:rep:updateregion'] && OldImage && !OldImage['aws:rep:updateregion']) {
    // skip
    return false;
  }

  // v2
  if (NewImage && NewImage.awsregion && OldImage && !OldImage.awsregion) {
    // skip
    return false;
  }

  return true;
};

// test helper
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

export const UNKNOWN_DYNAMODB_EVENT_TYPE = toDynamodbRecords([
  {
    timestamp: 0,
    keys: { pk: '0', sk: '0' },
    newImage: {
      discriminator: 'unknown',
    },
  },
]);
