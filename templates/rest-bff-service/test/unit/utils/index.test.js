import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { now } from '../../../src/utils';

describe('utils/index.js', () => {
  afterEach(sinon.restore);

  it('should test successful handle call', async () => {
    sinon.stub(Date, 'now').returns(1600144863435);
    expect(now()).to.equal(1600144863435);
  });
});
