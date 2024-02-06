import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { defaultDebugLogger } from '../../../src/utils';

describe('utils/log.js', () => {
  afterEach(sinon.restore);

  it('should replace newlines', () => {
    const debug = sinon.spy();

    const logger = defaultDebugLogger(debug);
    logger.info('Multi\nline\ntest.');

    expect(debug).to.have.been.calledWith('Multi\rline\rtest.');
  });

  it('should print json with max depth', () => {
    const debug = sinon.spy();

    const logger = defaultDebugLogger(debug);
    logger.info({ this: { is: { a: { test: { tooDeep: true } } } } });

    expect(debug).to.have.been.calledWith('{\r  this: { is: { a: { test: [Object] } } }\r}');
  });

  it('should only ignore client debug messages', () => {
    const debug = sinon.spy();
    const logger = defaultDebugLogger(debug);

    logger.debug('test1');
    logger.info('test2');
    logger.warn('test3');
    logger.error('test4');

    expect(debug).to.not.have.been.calledWith('test1');
    expect(debug).to.have.been.calledWith('test2');
    expect(debug).to.have.been.calledWith('test3');
    expect(debug).to.have.been.calledWith('test4');
  });
});
