import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  throwFault, rejectWithFault, faultify, faulty, faultyasync, faultyAsyncStream,
} from '../../../src/utils/faults';

describe('utils/faults.js', () => {
  afterEach(sinon.restore);

  it('should throw a fault', () => {
    const uow = {};
    try {
      throwFault(uow)(new Error('fault'));
    } catch (err) {
      expect(err.uow).to.deep.equal(uow);
      return;
    }
    throw new Error('failed throwFault');
  });

  it('should reject with a fault', async () => {
    const uow = {};
    try {
      await rejectWithFault(uow)(new Error('rejectWithFault'));
    } catch (err) {
      expect(err.uow).to.deep.equal(uow);
      return;
    }
    throw new Error('failed rejectWithFault');
  });

  it('should handle faulty synchronous function', () => {
    const uow = {};
    try {
      const f = () => { throw new Error('faulty'); };
      faulty(f)(uow);
    } catch (err) {
      expect(err.uow).to.deep.equal(uow);
      return;
    }
    throw new Error('failed faulty');
  });

  it('should handle faulty asynchronous function', async () => {
    const uow = {};
    try {
      const f = () => Promise.reject(new Error('faultyasync'));
      await faultyasync(f)(uow);
    } catch (err) {
      expect(err.uow).to.deep.equal(uow);
      return;
    }
    throw new Error('failed faultyasync');
  });

  it('should handle faultify', async () => {
    const uow = {};
    try {
      const f = faultify(() => { throw new Error('faultify sync'); });
      await faultyasync(f)(uow);
    } catch (err) {
      expect(err.uow).to.deep.equal(uow);
      return;
    }
    throw new Error('failed faultify');
  });

  it('should handle faulty synchronous function in stream', (done) => {
    const uows = [{
    }];

    const toUpdateRequest = (rule) => faulty((uow) => ({
      ...uow,
      updateRequest: rule.toUpdateRequest(uow, rule),
    }), false);

    _(uows)
      .flatMap(toUpdateRequest({
        toUpdateRequest: () => { throw new Error('faulty in stream'); },
      }))
      // .tap((uow) => console.log(JSON.stringify(uow, null, 2)))
      .errors((err, push) => {
        // console.log(JSON.stringify(err, null, 2));
        if (!err.uow) {
          push(err);
        } else {
          expect(err.uow).to.deep.equal(uows[0]);
        }
      })
      .done(done);
  });

  it('should handle faulty asynchronous function in stream', (done) => {
    const uows = [{
    }];

    const toUpdateRequest = (rule) => faultyAsyncStream(async (uow) => ({
      ...uow,
      updateRequest: await rule.toUpdateRequest(uow, rule),
    }), false);

    _(uows)
      .flatMap(toUpdateRequest({
        toUpdateRequest: () => Promise.reject(new Error('faultyasync in stream')),
      }))
      // .tap((uow) => console.log(JSON.stringify(uow, null, 2)))
      .errors((err, push) => {
        // console.log(JSON.stringify(err, null, 2));
        if (!err.uow) {
          push(err);
        } else {
          expect(err.uow).to.deep.equal(uows[0]);
        }
      })
      .done(done);
  });

  it('should handle faulty async or async in stream', (done) => {
    const uows = [{
    }];

    const toUpdateRequest = (rule) => faultyAsyncStream(async (uow) => ({
      ...uow,
      updateRequest: await faultify(rule.toUpdateRequest)(uow, rule),
    }), false);

    _(uows)
      .flatMap(toUpdateRequest({
        toUpdateRequest: () => { throw new Error('faulty or faultyasync in stream'); },
      }))
      // .tap((uow) => console.log(JSON.stringify(uow, null, 2)))
      .errors((err, push) => {
        // console.log(JSON.stringify(err, null, 2));
        if (!err.uow) {
          push(err);
        } else {
          expect(err.uow).to.deep.equal(uows[0]);
        }
      })
      .done(done);
  });
});
