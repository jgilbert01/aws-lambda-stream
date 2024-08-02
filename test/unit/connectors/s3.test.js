import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';
import { Readable } from 'stream';
import { mockClient } from 'aws-sdk-client-mock';
import {
  CopyObjectCommand,
  DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client,
} from '@aws-sdk/client-s3';
import { sdkStreamMixin } from '@smithy/util-stream';

import { v4 } from 'uuid';
import Connector from '../../../src/connectors/s3';

import { debug } from '../../../src/utils';

describe('connectors/s3.js', () => {
  let mockS3 = mockClient(S3Client);

  beforeEach(() => {
    mockS3 = mockClient(S3Client);
  });

  afterEach(() => {
    mockS3.restore();
    sinon.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should accept additional client options', async () => {
    // Don't use mock for this one test...
    mockS3.restore();

    const connector = new Connector({
      pipelineId: v4(),
      debug: debug('s3'),
      bucketName: 'b1',
      additionalClientOpts: {
        followRegionRedirects: true,
        bucketEndpoint: true,
      },
    });

    expect(connector.client.config.followRegionRedirects).to.eq(true);
    expect(connector.client.config.bucketEndpoint).to.eq(true);
  });

  it('should put object', async () => {
    const spy = sinon.spy(() => ({}));
    mockS3.on(PutObjectCommand).callsFake(spy);

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
    const spy = sinon.spy(() => ({ Body: sdkStreamMixin(Readable.from(Buffer.from('b'))) }));
    mockS3.on(GetObjectCommand).callsFake(spy);

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

  it('should get object as stream', (done) => {
    const spy = sinon.spy(() => ({ Body: sdkStreamMixin(Readable.from(Buffer.from('data'))) }));
    mockS3.on(GetObjectCommand).callsFake(spy);

    const inputParams = {
      Key: 'k1',
    };

    const objectStream = new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    }).getObjectStream(inputParams);

    _(objectStream)
      .flatMap((readable) => _(readable))
      .collect()
      .tap((collected) => {
        expect(collected[0]).to.be.instanceOf(Buffer);
        expect(collected[0].toString()).to.equal('data');
      })
      .done(done);
  });

  it('should delete object', async () => {
    const spy = sinon.spy(() => ({ DeleteMarker: false }));
    mockS3.on(DeleteObjectCommand).callsFake(spy);

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
    const spy = sinon.spy(() => ({
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
    mockS3.on(ListObjectsV2Command).callsFake(spy);

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

  it('should copy object', async () => {
    const spy = sinon.spy(() => ({}));
    mockS3.on(CopyObjectCommand).callsFake(spy);

    const inputParams = {
      Key: 'k1',
      CopySource: '/copysource/test-k1',
    };

    const data = await new Connector({
      debug: debug('s3'),
      bucketName: 'b1',
    }).copyObject(inputParams);

    expect(spy).to.have.been.calledWith({
      Bucket: 'b1',
      Key: 'k1',
      CopySource: '/copysource/test-k1',
    });
    expect(data).to.deep.equal({});
  });
});
