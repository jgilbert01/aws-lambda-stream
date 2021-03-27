import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  putObjectToS3, getObjectFromS3, listObjectsFromS3, pageObjectsFromS3,
} from '../../../src/utils/s3';

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

  it('should get object', (done) => {
    const stub = sinon.stub(Connector.prototype, 'getObject').resolves({
      Body: JSON.stringify({ f1: 'v1' }),
    });

    const uows = [{
      getRequest: {
        Key: 'k1',
      },
    }];

    _(uows)
      .through(getObjectFromS3())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Key: 'k1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          getRequest: {
            Key: 'k1',
          },
          getResponse: {
            Body: JSON.stringify({ f1: 'v1' }),
          },
        });
      })
      .done(done);
  });

  it('should list objects', (done) => {
    const stub = sinon.stub(Connector.prototype, 'listObjects').resolves({
      IsTruncated: false,
      Marker: '',
      Contents: [
        {
          Key: 'p1/2021/03/26/19/1234',
        },
      ],
    });

    const uows = [{
      listRequest: {
        Prefix: 'p1',
      },
    }];

    _(uows)
      .through(listObjectsFromS3())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Prefix: 'p1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          listRequest: {
            Prefix: 'p1',
          },
          listResponse: {
            IsTruncated: false,
            Marker: '',
            Contents: [
              {
                Key: 'p1/2021/03/26/19/1234',
              },
            ],
          },
        });
      })
      .done(done);
  });

  it.only('should page objects', (done) => {
    const responses = [
      {
        IsTruncated: true,
        NextMarker: 'm1',
        Contents: [
          {
            Key: 'p1/2021/03/26/19/1234',
          },
          {
            Key: 'p1/2021/03/26/19/5678',
          },
        ],
      },
      {
        IsTruncated: false,
        NextMarker: undefined,
        Contents: [
          {
            Key: 'p1/2021/03/26/19/9012',
          },
        ],
      },
    ];

    const stub = sinon.stub(Connector.prototype, 'listObjects')
      .callsFake(() => Promise.resolve((responses.shift())));

    const uows = [{
      listRequest: {
        Prefix: 'p1',
      },
    }];

    _(uows)
      .through(pageObjectsFromS3())
      .collect()
      .tap((collected) => {
        console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Prefix: 'p1',
          Marker: undefined,
        });
        expect(stub).to.have.been.calledWith({
          Prefix: 'p1',
          Marker: 'm1',
        });

        expect(collected.length).to.equal(3);
        expect(collected[0]).to.deep.equal({
          listRequest: {
            Prefix: 'p1',
            Marker: undefined,
          },
          listResponse: {
            IsTruncated: true,
            NextMarker: 'm1',
            Content: {
              Key: 'p1/2021/03/26/19/1234',
            },
          },
        });
        expect(collected[1]).to.deep.equal({
          listRequest: {
            Prefix: 'p1',
            Marker: undefined,
          },
          listResponse: {
            IsTruncated: true,
            NextMarker: 'm1',
            Content: {
              Key: 'p1/2021/03/26/19/5678',
            },
          },
        });
        expect(collected[2]).to.deep.equal({
          listRequest: {
            Prefix: 'p1',
            Marker: 'm1',
          },
          listResponse: {
            IsTruncated: false,
            NextMarker: undefined,
            Content: {
              Key: 'p1/2021/03/26/19/9012',
            },
          },
        });
      })
      .done(done);
  });
});
