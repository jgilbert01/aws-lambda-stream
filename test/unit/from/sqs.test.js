import 'mocha';
import { expect } from 'chai';

import {
  fromSqs, toSqsRecords, fromSqsEvent, toSqsEventRecords,
} from '../../../src/from/sqs';
import { fromS3, toS3Records } from '../../../src/from/s3';

describe('from/sqs.js', () => {
  it('should parse records', (done) => {
    const event = toSqsRecords([
      {
        body: 'this is a test',
        timestamp: '1595616620000',
      },
    ]);

    fromSqs(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventSource: 'aws:sqs',
            awsRegion: 'us-west-2',
            messageId: '00000000-0000-0000-0000-000000000000',
            body: 'this is a test',
            attributes: {
              SentTimestamp: '1595616620000',
            },
          },
        });
      })
      .done(done);
  });

  it('should parse s3 wrapped in sns wrapped in sqs records', (done) => {
    const event = toSqsRecords([
      {
        body: JSON.stringify({
          Message: JSON.stringify(toS3Records([{
            bucket: {
              name: 'b1',
            },
            object: {
              key: 'k1',
            },
          }])),
        }),
      },
    ]);

    fromSqs(event)
      .map((uow) => ({
        sqsRecord: uow.record,
        record: JSON.parse(uow.record.body),
      }))
      .map((uow) => ({
        ...uow,
        snsRecord: uow.record,
        record: JSON.parse(uow.record.Message),
      }))
      .flatMap((uow) => fromS3(uow.record)
        .map((record) => ({
          ...uow,
          ...record,
        })))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          sqsRecord: {
            eventSource: 'aws:sqs',
            messageId: '00000000-0000-0000-0000-000000000000',
            body: '{"Message":"{\\"Records\\":[{\\"eventSource\\":\\"aws:s3\\",\\"awsRegion\\":\\"us-west-2\\",\\"responseElements\\":{\\"x-amz-request-id\\":\\"0000000000000000\\"},\\"s3\\":{\\"bucket\\":{\\"name\\":\\"b1\\"},\\"object\\":{\\"key\\":\\"k1\\"}}}]}"}',
            attributes: {
              SentTimestamp: '1545082649183',
            },
            awsRegion: 'us-west-2',
          },
          snsRecord: {
            Message: '{"Records":[{"eventSource":"aws:s3","awsRegion":"us-west-2","responseElements":{"x-amz-request-id":"0000000000000000"},"s3":{"bucket":{"name":"b1"},"object":{"key":"k1"}}}]}',
          },
          record: {
            eventSource: 'aws:s3',
            awsRegion: 'us-west-2',
            responseElements: {
              'x-amz-request-id': '0000000000000000',
            },
            s3: {
              bucket: {
                name: 'b1',
              },
              object: {
                key: 'k1',
              },
            },
          },
        });
      })
      .done(done);
  });

  it('should parse event records', (done) => {
    const event = toSqsEventRecords([
      {
        type: 'thing-created',
        timestamp: '1595616620000',
        thing: {
          name: 'thing1',
        },
      },
      {
        type: 'thing-created',
        tags: {
          skip: true,
        },
      },
    ]);

    fromSqsEvent(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventSource: 'aws:sqs',
            awsRegion: 'us-west-2',
            messageId: '00000000-0000-0000-0000-000000000000',
            body: '{"type":"thing-created","timestamp":"1595616620000","thing":{"name":"thing1"}}',
            attributes: {
              SentTimestamp: '1595616620000',
            },
          },
          event: {
            id: '00000000-0000-0000-0000-000000000000',
            type: 'thing-created',
            timestamp: '1595616620000',
            thing: {
              name: 'thing1',
            },
          },
        });
      })
      .done(done);
  });
});
