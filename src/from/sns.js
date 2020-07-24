import _ from 'highland';

// this from function is intended for use with intra-service messages
// as opposd to consuming inter-servic events

export const fromSns = (event) =>
  _(event.Records)
    .map((record) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        record,
      }));

// test helper
// https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
export const toSnsRecords = (messages) => ({
  Records: messages.map((m, i) =>
    ({
      // EventVersion: '1.0',
      // EventSubscriptionArn: 'arn:aws:sns:us-east-2:123456789012:sns-lambda:21be56ed-a058-49f5-8c98-aedd2564c486',
      EventSource: 'aws:sns',
      Sns: {
        // SignatureVersion: '1',
        // Timestamp: '2019-01-02T12:45:07.000Z',
        // Signature: 'tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==',
        // SigningCertUrl: 'https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem',

        MessageId: `00000000-0000-0000-0000-00000000000${i}`,
        Message: m.msg,

        MessageAttributes: m.attributes || /* istanbul ignore next */ {
          // Test: {
          //   Type: 'String',
          //   Value: 'TestString',
          // },
          // TestBinary: {
          //   Type: 'Binary',
          //   Value: 'TestBinary',
          // },
        },
        // Type: 'Notification',
        // UnsubscribeUrl: 'https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486',
        // TopicArn: 'arn:aws:sns:us-east-2:123456789012:sns-lambda',
        Subject: m.subject || /* istanbul ignore next */ 'TestSubject',
      },
    })),
});
