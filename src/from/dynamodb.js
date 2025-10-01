/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import _ from 'highland';

import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { faulty, options } from '../utils';

export const fromDynamodb = (event, {
  pkFn = 'pk',
  skFn = 'sk',
  discriminatorFn = 'discriminator',
  eventTypePrefix = undefined,
  ignoreTtlExpiredEvents = false,
  ignoreReplicas = true,
  preferApproximateTimestamp = false,
} = {}) => // eslint-disable-line import/prefer-default-export

  // prepare the event stream
  _(event.Records)

    //--------------------------------
    // global table support
    .filter(outReplicas(ignoreReplicas))
    .filter(outGlobalTableExtraModify)
    //--------------------------------
    // ttl support
    .filter(outTtlExpiredEvents(ignoreTtlExpiredEvents))
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
          timestamp: deriveTimestamp(record, preferApproximateTimestamp),
          tags: {
            region: record.awsRegion,
          },
          raw: {
            new: record.dynamodb.NewImage
              ? unmarshall(record.dynamodb.NewImage)
              : undefined,
            old: record.dynamodb.OldImage
              ? unmarshall(record.dynamodb.OldImage)
              : undefined,
          },
        },
      })))
    .tap((uow) => options().metrics?.adornDynamoMetrics(uow, event));

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

const deriveTimestamp = (record, preferApproximateTimestamp) => {
  if (preferApproximateTimestamp) {
    return ddbApproximateCreationTimestamp(record);
  } else {
    return parseInt(record.dynamodb.NewImage?.timestamp?.N, 10) || ddbApproximateCreationTimestamp(record);
  }
};

export const ddbApproximateCreationTimestamp = (record) => record.dynamodb.ApproximateCreationDateTime * 1000;

//--------------------------------------------
// global table support - version: 2017.11.29
//--------------------------------------------
// or use the following:
//
// filterPatterns:
//   - eventName: [ INSERT, MODIFY ]
//     dynamodb:
//       NewImage:
//         awsregion:
//           S:
//             - ${opt:region}
//   - eventName: [ REMOVE ]
//     dynamodb:
//       OldImage:
//         awsregion:
//           S:
//             - ${opt:region}

export const outReplicas = (ignoreReplicas) => (record) => {
  if (!ignoreReplicas) return true;

  const image = record.dynamodb.NewImage || record.dynamodb.OldImage;

  // is this a global table event
  // v2
  if (image.awsregion) {
    // only process events from the current region
    return image.awsregion.S === process.env.AWS_REGION;
  }

  // v1
  /* istanbul ignore next */
  if (image['aws:rep:updateregion']) {
    // only process events from the current region
    return image['aws:rep:updateregion'].S === process.env.AWS_REGION;
  }

  return true;
};

// dynamodb stream emits extra events as it adorns the 'aws:rep' global table metadata
export const outGlobalTableExtraModify = (record) => {
  const { NewImage, OldImage } = record.dynamodb;

  // v1/v2 transition
  if (NewImage && NewImage.awsregion && OldImage && (!OldImage.awsregion && !OldImage['aws:rep:updateregion'])) {
    // skip
    return false;
  }

  // v1
  /* istanbul ignore next */
  if (NewImage && NewImage['aws:rep:updateregion'] && OldImage && !OldImage['aws:rep:updateregion']) {
    // skip
    return false;
  }

  return true;
};

//--------------------------------------------
// ttl support or use filterPatterns
//--------------------------------------------

export const outTtlExpiredEvents = (ignoreTtlExpiredEvents) => (record) => {
  const { eventName, userIdentity, dynamodb: { OldImage, ApproximateCreationDateTime } } = record;
  // this is not a REMOVE event or we're not ignoring the ttl expired events anyway.
  if (eventName !== 'REMOVE' || !ignoreTtlExpiredEvents) return true;

  if (userIdentity) {
    // See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
    // We trust dynamodb that the ttl expired if its a remove and has the ttl expiry indicating
    // identity attributes.
    return !(userIdentity?.type === 'Service' && userIdentity?.principalId === 'dynamodb.amazonaws.com');
  } else if (OldImage.ttl?.N) {
    // If no user identity attribute is present, this may be a replicated TTL delete, but we still
    // want to honor it because filtering out replica region events may be disabled.
    const ttlSec = Number(OldImage.ttl.N);
    return !(ttlSec <= ApproximateCreationDateTime);
  } else {
    return true;
  }
};

// test helper
export const toDynamodbRecords = (events, { removeUndefinedValues = true } = {}) => ({
  Records: events.map((e, i) =>
    ({
      eventID: `${i}`,
      eventName: !e.oldImage ? 'INSERT' : !e.newImage ? 'REMOVE' : 'MODIFY', // eslint-disable-line no-nested-ternary
      // eventVersion: '1.0',
      eventSource: 'aws:dynamodb',
      awsRegion: e.newImage?.awsregion || process.env.AWS_REGION || /* istanbul ignore next */ 'us-west-2',
      dynamodb: {
        ApproximateCreationDateTime: e.timestamp,
        Keys: e.keys ? marshall(e.keys, { removeUndefinedValues }) : /* istanbul ignore next */ undefined,
        NewImage: e.newImage ? marshall(e.newImage, { removeUndefinedValues }) : undefined,
        OldImage: e.oldImage ? marshall(e.oldImage, { removeUndefinedValues }) : undefined,

        SequenceNumber: `${i}`,
        // SizeBytes: 59,
        StreamViewType: 'NEW_AND_OLD_IMAGES',
      },
      // eventSourceARN: 'arn:aws:dynamodb:us-west-2:123456789012:table/myservice-entities/stream/2016-11-16T20:42:48.104',
      ...(e.ttlDelete && {
        userIdentity: {
          principalId: 'dynamodb.amazonaws.com',
          type: 'Service',
        },
      }),
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
