import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import debug from 'debug';
import _ from 'highland';

import S3Connector from '../../../src/connectors/s3';
import { submitClaimcheck } from '../../../src/sinks/claimcheck';

describe('sinks/claimcheck.js', () => {
  let claimcheckStub;

  beforeEach(() => {
    claimcheckStub = sinon.stub(S3Connector.prototype, 'putObject').resolves({});
  });

  afterEach(sinon.restore);

  it('should bypass claimcheck if no bucket set', (done) => {
    delete process.env.CLAIMCHECK_BUCKET_NAME;

    const uow = {
      claimcheckRequired: true,
      event: {
        id: 'i1',
        type: 'e1',
        timestamp: 12345,
        partitionKey: 'p1',
        attr: 'a',
      },
    };

    _([uow])
      .through(submitClaimcheck({
        id: 'test',
        debug: debug('test'),
        step: 'test',
        eventField: 'event',
        claimcheckEventField: 'claimcheckEvent',
        claimcheckRequiredField: 'claimcheckRequired',
      }))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        // If nowhere to claimcheck to, should just be a passthrough.
        expect(collected[0]).to.deep.equal(uow);
        expect(claimcheckStub.notCalled);
      })
      .done(done);
  });

  it('should claimcheck if uow indicates', (done) => {
    process.env.CLAIMCHECK_BUCKET_NAME = 'test-bucket';

    const uow = {
      claimcheckRequired: true,
      event: {
        id: 'i1',
        type: 'e1',
        timestamp: 12345,
        partitionKey: 'p1',
        attr: 'a',
      },
    };

    _([uow])
      .through(submitClaimcheck({
        id: 'test',
        debug: debug('test'),
        step: 'test',
        eventField: 'event',
        claimcheckEventField: 'claimcheckEvent',
        claimcheckRequiredField: 'claimcheckRequired',
      }))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0]).to.deep.equal({
          claimcheckEvent: {
            id: 'i1',
            partitionKey: 'p1',
            s3: {
              bucket: 'test-bucket',
              key: 'CLAIMCHECK-i1',
            },
            timestamp: 12345,
            type: 'e1',
          },
          claimcheckRequired: true,
          event: {
            attr: 'a',
            id: 'i1',
            partitionKey: 'p1',
            timestamp: 12345,
            type: 'e1',
          },
          putClaimcheckRequest: {
            Body: '{"id":"i1","type":"e1","timestamp":12345,"partitionKey":"p1","attr":"a"}',
            Key: 'CLAIMCHECK-i1',
          },
          putClaimcheckResponse: {},
        });
        expect(claimcheckStub.calledOnce);
      })
      .done(done);
  });
});
