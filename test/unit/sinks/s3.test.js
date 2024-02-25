import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  putObjectToS3,
  deleteObjectFromS3,
} from '../../../src/sinks/s3';

import Connector from '../../../src/connectors/s3';

describe('utils/s3.js', () => {
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
});
