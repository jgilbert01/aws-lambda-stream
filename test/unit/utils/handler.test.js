import 'mocha';
import { expect } from 'chai';

import _ from 'highland';

import { toCallback, toPromise } from '../../../src/utils/handler';

describe('utils/handler.js', () => {
  it('should handle with promise', async () => {
    const handlerWithPromise = async (event, context) =>
      _(event.Records)
        .through(toPromise);

    const result = await handlerWithPromise({
      Records: ['r11', 'r12'],
    }, {});

    expect(result).to.equal('Success');
  });

  it('should handle with callback', (done) => {
    const handlerWithCallback = (event, context, cb) =>
      _(event.Records)
        .through(toCallback(cb));

    handlerWithCallback({
      Records: ['r21', 'r22'],
    }, {}, (err, result) => {
      expect(result).to.equal('Success');
      done(err);
    });
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

    handlerWithCallback({}, {}, (err) => {
      if (err) done();
      if (!err) done('failed');
    });
  });
});
