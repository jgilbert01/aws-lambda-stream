import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import debug from 'debug';
import {
  fromS3, fromSqsS3, fromSqsSnsS3, fromS3Event, toS3Records, toSqsS3Records, toSqsSnsS3Records,
} from '../../../src/from/s3';

import Connector from '../../../src/connectors/s3';
import { toPromise } from '../../../src/utils';

describe('from/s3s.js', () => {
  afterEach(() => {
    sinon.restore();
  });

  it('should parse records', (done) => {
    const event = toS3Records([
      {
        bucket: {
          name: 'b1',
        },
        object: {
          key: 'k1',
        },
      },
    ]);

    fromS3(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
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

  it('should parse sqs, s3 records', (done) => {
    const event = toSqsS3Records([{
      bucket: {
        name: 'mfe-main-stg-websitebucket-wrivy8kb9743',
      },
      object: {
        key: 'mfe-main/stg/mfe.json',
      },
    }]);

    fromSqsS3(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].record.s3).to.deep.equal({
          s3: {
            bucket: {
              name: 'mfe-main-stg-websitebucket-wrivy8kb9743',
            },
            object: {
              key: 'mfe-main/stg/mfe.json',
            },
          },
        });
      })
      .done(done);
  });

  it('should parse sqs sns, s3 records', (done) => {
    // const event = {
    //   Records: [
    //     {
    //       messageId: '0e71562d-befa-4e60-876a-d9cc44e0535d',
    //       receiptHandle: 'AQEBgYDcil+eVBLrKVVjVuVy19MfzdVXhpz1iG8JydYFudWssjm8aUrI2/Jn7rn/llK1cTRey2zQ+OAFxuJF/24A0hpUra27/r4UXuxSvTpxdfUhOAdjhHX+GG3zFWi/HghC/pXTNqRdhkfnvY6MF/ykHWTKQ1jX8VKVGxCXZpeGgUqAILoEqEN5hRPYTZFfzzaLNz4LZmkBHDvosCXM+WWBRBOVAeTV+c9/39JGgAbumVgfXoYI0ajZb3cCmSzLwO3p5MwPR5K7Q1gHu/+CYJM4Z13moAMrlfaZk/xksSRGhOF5NHrTx4bmFyFRiyLtxiMXvnsqfuCWgDFgAsq2ExCYPkYZRYk0M1awrIpbG1us5oa3p+NF5RYnfj34pI+EsuCVDhpCXonvw5DiJ0zZpYCCPNKCmKnfRo5daqBK+ozILTtbSRIQRa0/1PMwzM1sdXjC',
    //       body: '{\n  "Type" : "Notification",\n  "MessageId" : "43d05c24-b26e-552e-8679-04810fb365ed",\n  "TopicArn" : "arn:aws:sns:us-east-1:026257137139:mfe-main-stg-metadata-topic",\n  "Subject" : "Amazon S3 Notification",\n  "Message" : "{\\"Records\\":[{\\"eventVersion\\":\\"2.1\\",\\"eventSource\\":\\"aws:s3\\",\\"awsRegion\\":\\"us-east-1\\",\\"eventTime\\":\\"2021-02-16T12:33:24.880Z\\",\\"eventName\\":\\"ObjectCreated:Put\\",\\"userIdentity\\":{\\"principalId\\":\\"AWS:AIDAQMHIMDXZUZ24KCFNF\\"},\\"requestParameters\\":{\\"sourceIPAddress\\":\\"173.66.80.214\\"},\\"responseElements\\":{\\"x-amz-request-id\\":\\"F1299AFE13C549F3\\",\\"x-amz-id-2\\":\\"0pRQ8WUagn0rmBk1fufdSuMN+e66vAj77GEVGor0+9H/ixI8TSERbgU91piV04jRR5J4oKbznJvRNX03QgXMMviqEO+9NKe/tck0TOfZaB0=\\"},\\"s3\\":{\\"s3SchemaVersion\\":\\"1.0\\",\\"configurationId\\":\\"c2a9f34a-1968-4167-b129-c6d5a4ca1225\\",\\"bucket\\":{\\"name\\":\\"mfe-main-stg-websitebucket-wrivy8kb9743\\",\\"ownerIdentity\\":{\\"principalId\\":\\"A33VIG69HLWPQP\\"},\\"arn\\":\\"arn:aws:s3:::mfe-main-stg-websitebucket-wrivy8kb9743\\"},\\"object\\":{\\"key\\":\\"mfe-main/stg/mfe.json\\",\\"size\\":1345,\\"eTag\\":\\"fb01bb53ca3ff1d1700b799dd0f86d93\\",\\"sequencer\\":\\"00602BBB9B8070806C\\"}}}]}",\n  "Timestamp" : "2021-02-16T12:33:33.131Z",\n  "SignatureVersion" : "1",\n  "Signature" : "Qv5EfA85m2w8lsguAEMa0gmI/YYM/gAZcyyJ32xBFjI7qtssZYex93fVuFp0h9Xq40VlLT6CE1itK2GxzBCPBjRH1x3hACEisc1c64YmB1JZUmlAvfK/cBaqa5EUGxHi5nuc2dxb/el0EBe4cIIjGnDR+vI/kTB/RGNrN+40h5MGoxIm3Mqbu5Ib4T0hB96UZ+lJZUgm/CmNrsXerrQpXBCE006hgD2J3sq6zdRMEifNlGSsXD/SVPIFE1I5umCbTEU/dT3Qb7Y1PISGbuEqMzLnTzAosySz9P2BDC7fXpEKmXKi9UNlXnzn3gkOUVGFnukS0zijGzfKW4q/9iMNWQ==",\n  "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",\n  "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:026257137139:mfe-main-stg-metadata-topic:3bfec31c-0ba5-41a3-9c43-c274a2ad501c"\n}',
    //       attributes: {
    //         ApproximateReceiveCount: '1',
    //         SentTimestamp: '1613478813187',
    //         SenderId: 'AIDAIT2UOQQY3AUEKVGXU',
    //         ApproximateFirstReceiveTimestamp: '1613478813200',
    //       },
    //       messageAttributes: {},
    //       md5OfBody: 'bb23c06577a5d852a162803c0a6f3fb2',
    //       eventSource: 'aws:sqs',
    //       eventSourceARN: 'arn:aws:sqs:us-east-1:026257137139:mfe-metadata-deployer-stg-Queue-1DFHTK2TBQSF2',
    //       awsRegion: 'us-east-1',
    //     },
    //   ],
    // };

    const event = toSqsSnsS3Records([{
      bucket: {
        name: 'mfe-main-stg-websitebucket-wrivy8kb9743',
      },
      object: {
        key: 'mfe-main/stg/mfe.json',
      },
    }]);

    fromSqsSnsS3(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].record.s3).to.deep.equal({
          // eventVersion: '2.1',
          // eventSource: 'aws:s3',
          // awsRegion: 'us-east-1',
          // eventTime: '2021-02-16T12:33:24.880Z',
          // eventName: 'ObjectCreated:Put',
          // userIdentity: {
          //   principalId: 'AWS:AIDAQMHIMDXZUZ24KCFNF',
          // },
          // requestParameters: {
          //   sourceIPAddress: '173.66.80.214',
          // },
          // responseElements: {
          //   'x-amz-request-id': 'F1299AFE13C549F3',
          //   'x-amz-id-2': '0pRQ8WUagn0rmBk1fufdSuMN+e66vAj77GEVGor0+9H/ixI8TSERbgU91piV04jRR5J4oKbznJvRNX03QgXMMviqEO+9NKe/tck0TOfZaB0=',
          // },
          s3: {
            // s3SchemaVersion: '1.0',
            // configurationId: 'c2a9f34a-1968-4167-b129-c6d5a4ca1225',
            bucket: {
              name: 'mfe-main-stg-websitebucket-wrivy8kb9743',
              // ownerIdentity: {
              //   principalId: 'A33VIG69HLWPQP',
              // },
              // arn: 'arn:aws:s3:::mfe-main-stg-websitebucket-wrivy8kb9743',
            },
            object: {
              key: 'mfe-main/stg/mfe.json',
              // size: 1345,
              // eTag: 'fb01bb53ca3ff1d1700b799dd0f86d93',
              // sequencer: '00602BBB9B8070806C',
            },
          },
        });
      })
      .done(done);
  });

  it('should parse event records', (done) => {
    sinon.stub(Connector.prototype, 'getObject').resolves({
      Key: '1/thing',
      Body: Buffer.from(JSON.stringify({
        id: '00000000-0000-0000-0000-000000000000',
        type: 'thing-created',
        timestamp: '1595616620000',
        thing: {
          name: 'thing1',
        },
      })),
    });

    const event = toSqsSnsS3Records([{
      bucket: {
        name: 'my-bucket',
      },
      object: {
        key: '1/thing',
      },
    }]);

    fromS3Event(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(1);
        expect(collected[0].event).to.deep.equal({
          id: '00000000-0000-0000-0000-000000000000',
          type: 'thing-created',
          timestamp: '1595616620000',
          thing: {
            name: 'thing1',
          },
        });
      })
      .done(done);
  });

  it('should use a pipeline label to cache regional redirects configuration', async () => {
    sinon.stub(Connector.prototype, 'getObject').resolves({
      Key: '1/thing',
      Body: Buffer.from(JSON.stringify({
        id: '00000000-0000-0000-0000-000000000000',
        type: 'thing-created',
        timestamp: '1595616620000',
        thing: {
          name: 'thing1',
        },
      })),
    });

    const event = toSqsSnsS3Records([{
      bucket: {
        name: 'my-bucket',
      },
      object: {
        key: '1/thing',
      },
    }]);

    await fromS3Event(event)
      .collect()
      .through(toPromise);

    const testClient = new Connector({ debug: debug('test'), bucketName: 'test-bucket', pipelineId: 'handler:fromS3' }).client;
    expect(testClient.config.followRegionRedirects).to.eq(true);
  });
});
