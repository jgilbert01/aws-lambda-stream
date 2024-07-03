import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import _ from 'highland';
import Promise from 'bluebird';

import { mw, toPromise } from '../../../src/utils/handler';

describe('utils/handler.js', () => {
  afterEach(sinon.restore);

  it('should execute middleware', async () => {
    const stub = sinon.stub(console, 'log');

    const handle = (event, context, options) => Promise.resolve({
      output: { value: event.value + 1 },
      context,
      options,
    });

    const result = await mw(handle, { opt1: 1 })
      .use((next, opt, evt, ctx) => {
        console.log('before: ', evt, ctx, opt);
        return next()
          .tap((resp) => console.log('after: ', resp));
      })
      .use(async (next, opt, evt, ctx) => {
        opt.opt2 = 2;
        ctx.before = await Promise.resolve(evt.value); // do something async before
        const resp = await next();
        ctx.after = await (async () => resp.output.value + 1)(); // do something async after
        return resp;
      })({ value: 1 }, { ctx1: '1' })
      .tap(() => { });

    expect(stub).to.have.been.called;
    expect(result).to.deep.equal({
      output: {
        value: 2,
      },
      context: {
        ctx1: '1',
        before: 1,
        after: 3,
      },
      options: {
        opt1: 1,
        opt2: 2,
      },
    });

    // coverage
    await mw(handle).use([
      (next) => next(),
      (next) => next(),
    ])({});
  });

  it('should handle with promise', async () => {
    const handlerWithPromise = async (event, context) =>
      _(event.Records)
        .through(toPromise);

    const result = await handlerWithPromise({
      Records: ['r11', 'r12'],
    }, {});

    expect(result).to.equal('Success');
  });

  it('should handle with promise reject', () => {
    const handlerWithPromise = async (event, context) =>
      _.fromError(new Error('Promise Reject'))
        .through(toPromise);

    return handlerWithPromise({}, {})
      .then(() => Promise.reject(new Error('failed')))
      .catch((err) => ('Caught'));
  });
});
