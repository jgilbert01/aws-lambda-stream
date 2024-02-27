import _ from 'highland';

import { getObjectFromS3 } from '../queries/s3';
import { decompress } from '../utils';

// this from function is intended for use with intra-service messages
// as opposed to consuming inter-servic events

export const fromS3 = (event) =>
  _(event.Records)
    .map((record) =>
    // create a unit-of-work for each message
    // so we can correlate related work for error handling
      ({
        record,
      }));

export const fromSqsSnsS3 = (event) =>
  _(event.Records)
    // sqs
    .map((record) =>
    // create a unit-of-work for each message
    // so we can correlate related work for error handling
      ({
        record: {
          sqs: record,
        },
      }))
    // sns
    .map((uow) => ({
      record: {
        ...uow.record,
        sns: JSON.parse(uow.record.sqs.body),
      },
    }))
    // s3
    .map((uow) => ({
      record: {
        ...uow.record,
        s3: JSON.parse(uow.record.sns.Message),
      },
    }))
    .flatMap((uow) => fromS3(uow.record.s3)
      .map((uow2) => ({
        record: {
          ...uow.record,
          s3: uow2.record,
        },
      })));

export const fromS3Event = (event, options = {}) =>
  fromSqsSnsS3(event)
    .map((uow) => ({
      ...uow,
      getRequest: {
        Bucket: uow.record.s3.s3.bucket.name,
        Key: uow.record.s3.s3.object.key,
      },
    }))
    .through(getObjectFromS3(options))
    .map((uow) => ({
      ...uow,
      event: JSON.parse(Buffer.from(uow.getResponse.Body), decompress),
    }));

// test helper
// https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html
export const toS3Records = (notifications) => ({
  Records: notifications.map((n, i) =>
    ({
    // eventVersion: '2.1',
      eventSource: 'aws:s3',
      awsRegion: 'us-west-2',
      // eventTime: '2019-09-03T19:37:27.192Z',
      // eventName: 'ObjectCreated:Put',
      // userIdentity: {
      //   principalId: 'AWS:AIDAINPONIXQXHT3IKHL2',
      // },
      // requestParameters: {
      //   sourceIPAddress: '205.255.255.255',
      // },
      responseElements: {
        'x-amz-request-id': `000000000000000${i}`,
      //   'x-amz-id-2': 'vlR7PnpV2Ce81l0PRw6jlUpck7Jo5ZsQjryTjKlc5aLWGVHPZLj5NeC6qMa0emYBDXOo6QBU0Wo=',
      },
      s3: {
      // s3SchemaVersion: '1.0',
      // configurationId: '828aa6fc-f7b5-4305-8584-487c791949c1',
        bucket: n.bucket, // {
        // name: 'lambda-artifacts-deafc19498e3f2df',
        // ownerIdentity: {
        //   principalId: 'A3I5XTEXAMAI3E',
        // },
        // arn: 'arn:aws:s3:::lambda-artifacts-deafc19498e3f2df',
        // },
        object: n.object, // {
      // key: 'b21b84d653bb07b05b1e6b33684dc11b',
      // size: 1305107,
      // eTag: 'b21b84d653bb07b05b1e6b33684dc11b',
      // sequencer: '0C0F6F405D6ED209E1',
      // },
      },
    })),
});

export const toSqsSnsS3Records = (notifications) => ({
  Records: ([{
    body: JSON.stringify({
      Message: JSON.stringify({
        Records: notifications.map((s3) => ({
          s3,
        })),
      }),
    }),
  }]),
});
