import _ from 'highland';

// this from function is intended for use with intra-service messages
// as opposd to consuming inter-servic events

export const fromSqs = (event) =>
  _(event.Records)
    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
      }));

// test helper
// https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
export const toSqsRecords = (messages) => ({
  Records: messages.map((m, i) =>
    ({
      eventSource: 'aws:sqs',
      messageId: `00000000-0000-0000-0000-00000000000${i}`,
      // receiptHandle: 'AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...',
      body: m.body,
      attributes: {
        // ApproximateReceiveCount: '1',
        SentTimestamp: m.timestamp || /* istanbul ignore next */ '1545082649183',
        // SenderId: 'AIDAIENQZJOLO23YVJ4VO',
        // ApproximateFirstReceiveTimestamp: '1545082649185'

        // SequenceNumber: '18849496460467696128',
        // MessageGroupId: '1',
        // MessageDeduplicationId: '1',

      },
      // messageAttributes: {},
      // md5OfBody: 'e4e68fb7bd0e697a0ae8f1bb342846b3',
      awsRegion: 'us-west-2',
      // eventSourceARN: 'arn:aws:sqs:us-west-2:123456789012:my-queue',
    })),
});
