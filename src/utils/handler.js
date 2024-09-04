import _ from 'highland';
import Promise from 'bluebird';

import { options } from './opt';

export const mw = (handle, opt) => {
  const stack = [];

  const run = (event, context) => {
    stack.push((n, o, e, c) => handle(e, c, o)); // do the real work last
    const runner = (index) => Promise.resolve(stack[index](() => runner(index + 1), opt, event, context));
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
