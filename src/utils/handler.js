import _ from 'highland';
import Promise from 'bluebird';

import { options } from './opt';

export const mw = (handle, opt) => {
  const stack = [];

  const run = (event, context) => {
    const mwStack = [...stack, (n, o, e, c) => handle(e, c, o)]; // Do the real work last
    const runner = (index) => Promise.resolve(mwStack[index](() => runner(index + 1), opt, event, context));
    return Promise.resolve(runner(0));
  };

  run.use = (middleware) => {
    stack.push(...(Array.isArray(middleware) ? middleware : [middleware]));
    return run;
  };

  return run;
};

export const toPromise = (s) => {
  const opt = options();
  if (opt.metrics) {
    return opt.metrics.toPromise(opt, s);
  } else {
    return new Promise((resolve, reject) => {
      s.consume((err, x, push, next) => {
        if (err) {
          reject(err);
        } else if (x === _.nil) {
          resolve('Success');
        } else {
          next();
        }
      })
        .resume();
    });
  }
};
