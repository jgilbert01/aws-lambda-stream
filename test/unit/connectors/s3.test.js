import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/s3';

import { debug } from '../../../src/utils';

describe('connectors/s3.js', () => {
  afterEach(() => {
    AWS.restore('S3');
  });

  it('should put object', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('S3', 'putObject', spy);

    const inputParams = {
      Body: JSON.stringify({ f1: 'v1' }),
      Key: 'k1',
    };

    const data = await new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    }).putObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Body: inputParams.Body,
      Key: 'k1',
    });
    expect(data).to.deep.equal({});
  });

  it('should get object', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { Body: 'b' }));
    AWS.mock('S3', 'getObject', spy);

    const inputParams = {
      Key: 'k1',
    };

    const data = await new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    }).getObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Key: 'k1',
    });
    expect(data).to.deep.equal({ Body: 'b' });
  });

  it('should delete object', async () => {
    const spy = sinon.spy((params, cb) => cb(null, { DeleteMarker: false }));
    AWS.mock('S3', 'deleteObject', spy);

    const inputParams = {
      Key: 'k1',
    };

    const data = await new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    })
      .deleteObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Key: 'k1',
      // VersionId: undefined,
    });
    expect(data).to.deep.equal({ DeleteMarker: false });
  });

  it('should list objects', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {
      IsTruncated: false,
      NextContinuationToken: '',
      Contents: [
        {
          Key: 'p1/2021/03/26/19/1234',
          LastModified: '2021-03-26T19:17:15.000Z',
          ETag: '"a192b6e6886f117cd4fa64168f6ec378"',
          Size: 1271,
          StorageClass: 'STANDARD',
          Owner: {},
        },
      ],
      Name: 'b1',
      Prefix: 'p1',
      MaxKeys: 1000,
      CommonPrefixes: [],
    }));
    AWS.mock('S3', 'listObjectsV2', spy);

    const inputParams = {
      Prefix: 'p1',
    };

    const data = await new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    }).listObjects(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Prefix: 'p1',
    });
    expect(data.Contents[0].Key).to.equal('p1/2021/03/26/19/1234');
  });
});
