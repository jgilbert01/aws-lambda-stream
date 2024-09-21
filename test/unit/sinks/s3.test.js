import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  putObjectToS3,
  deleteObjectFromS3,
  copyS3Object,
} from '../../../src/sinks/s3';

import Connector from '../../../src/connectors/s3';

describe('sinks/s3.js', () => {
  afterEach(sinon.restore);

  it('should put object', (done) => {
    const stub = sinon.stub(Connector.prototype, 'putObject').resolves({});

    const uows = [{
      putRequest: {
        Body: JSON.stringify({ f1: 'v1' }),
        Key: 'k1',
      },
    }];

    _(uows)
      .through(putObjectToS3())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Body: JSON.stringify({ f1: 'v1' }),
          Key: 'k1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          putRequest: {
            Body: JSON.stringify({ f1: 'v1' }),
            Key: 'k1',
          },
          putResponse: {},
        });
      })
      .done(done);
  });

  it('should copy object', (done) => {
    const stub = sinon.stub(Connector.prototype, 'copyObject').resolves({});

    const uows = [
      {
        copyRequest: {
          CopySource: '/sourcebucket/source-key',
          Key: 'destination-key',
        },
      },
      {},
    ];

    _(uows)
      .through(copyS3Object())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({ CopySource: '/sourcebucket/source-key', Key: 'destination-key' });

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal({
          copyRequest: { CopySource: '/sourcebucket/source-key', Key: 'destination-key' },
          copyResponse: {},
        });
        expect(collected[1]).to.deep.equal({});
      })
      .done(done);
  });

  it('should delete object', (done) => {
    const stub = sinon.stub(Connector.prototype, 'deleteObject').resolves({ DeleteMarker: false });

    const uows = [{
      deleteRequest: {
        Key: 'k1',
      },
    }];

    _(uows)
      .through(deleteObjectFromS3())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Key: 'k1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          deleteRequest: {
            Key: 'k1',
          },
          deleteResponse: { DeleteMarker: false },
        });
      })
      .done(done);
  });

  it('should passthrough additional client options to connector', () => {
    putObjectToS3({
      id: 'put-object-passthrough-test',
      additionalClientOpts: {
        followRegionRedirects: true,
        bucketEndpoint: true,
      },
    });

    const clientInstance = Connector.getClient('put-object-passthrough-test');
    expect(clientInstance.config.followRegionRedirects).to.eq(true);
    expect(clientInstance.config.bucketEndpoint).to.eq(true);
  });
});
