import 'mocha';
import { expect } from 'chai';

import { defaultBackoffDelay } from '../../../src/utils';

describe('utils/retry.js', () => {
  it('should exponentially backoff by default', () => {
    const attempt1 = defaultBackoffDelay(1);
    const attempt2 = defaultBackoffDelay(2);
    const attempt3 = defaultBackoffDelay(3);
    const attempt4 = defaultBackoffDelay(4);

    const attempt10 = defaultBackoffDelay(10);

    expect(attempt1).to.be.within(202, 402);
    expect(attempt2).to.be.within(204, 404);
    expect(attempt3).to.be.within(208, 408);
    expect(attempt4).to.be.within(216, 416);
    expect(attempt10).to.be.within(1224, 1424);
  });
});
