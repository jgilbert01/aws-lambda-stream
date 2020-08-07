import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { UNKNOWN_DYNAMODB_EVENT_TYPE } from 'aws-lambda-stream';

import { handle, Handler } from '../../../src/trigger';

describe('trigger/index.js', () => {
  afterEach(sinon.restore);

  it('should verify Handler', (done) => {
    new Handler()
      .handle(UNKNOWN_DYNAMODB_EVENT_TYPE)
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });

  it('should test successful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.of({}));

    const res = await handle({}, {});

    expect(spy).to.have.been.calledWith({});
    expect(res).to.equal('Success');
  });

  it('should test unsuccessful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.fromError(Error()));

    try {
      await handle({}, {});
      expect.fail('expected error');
    } catch (e) {
      expect(spy).to.have.been.calledWith({});
    }
  });
});
