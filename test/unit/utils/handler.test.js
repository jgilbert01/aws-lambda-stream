import 'mocha';
import sinon from 'sinon';

import _ from 'highland';

import { toCallback, toPromise } from '../../../src/utils/handler';

describe('utils/handler.js', () => {
  afterEach(sinon.restore);

  it('should handle with promise', () => {
    const handlerWithPromise = async (event, context) =>
      _(event.Records)
        .through(toPromise);

    return handlerWithPromise({
      Records: ['r11', 'r12'],
    }, {});
  });

  it('should handle with callback', (done) => {
    const handlerWithCallback = (event, context, cb) =>
      _(event.Records)
        .through(toCallback(cb));

    return handlerWithCallback({
      Records: ['r21', 'r22'],
    }, {}, done);
  });

  it('should handle with promise reject', () => {
    const handlerWithPromise = async (event, context) =>
      _.fromError(new Error('Promise Reject'))
        .through(toPromise);

    return handlerWithPromise({}, {})
      .then(() => Promise.reject(new Error('failed')))
      .catch((err) => ('Caught'));
  });

  it('should handle with callback error', (done) => {
    const handlerWithCallback = (event, context, cb) =>
      _.fromError(new Error('Callback Error'))
        .through(toCallback(cb));

    return handlerWithCallback({}, {}, (err) => {
      if (err) done();
      if (!err) done('failed');
    });
  });
});
