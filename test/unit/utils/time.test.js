import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { now, ttl, ttlRule } from '../../../src/utils';

describe('utils/time', () => {
  afterEach(sinon.restore);

  it('should get now timestamp', () => {
    sinon.stub(Date, 'now').returns(1540454400000);
    expect(now()).to.equal(1540454400000);
  });

  it('should calculate ttl', () => {
    expect(ttl(1540454400000, 11)).to.equal(1541404800);
  });

  it('should eval ttl rule', () => {
    const uow = { event: { timestamp: 1540454400000 } };

    const r1 = { ttl: 11 };
    expect(ttlRule(r1, uow)).to.equal(1541404800);

    const r2 = { ttl: (rule, uow) => (ttl(uow.event.timestamp, 11)) }; // eslint-disable-line no-shadow
    expect(ttlRule(r2, uow)).to.equal(1541404800);

    const r3 = { ttl: undefined };
    expect(ttlRule(r3, uow)).to.equal(1543305600);

    const r4 = { defaultTtl: 22 };
    expect(ttlRule(r4, uow)).to.equal(1542355200);
  });
});
