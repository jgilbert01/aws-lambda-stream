import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { now } from '../../../src/utils';

describe('utils', () => {
  afterEach(sinon.restore);

  it('should get now timestamp', () => {
    sinon.stub(Date, 'now').returns(1540454400000);
    expect(now()).to.equal(1540454400000);
  });
});
