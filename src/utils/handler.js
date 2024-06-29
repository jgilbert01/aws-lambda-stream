import _ from 'highland';
import Promise from 'bluebird';

import { options } from './opt';

export const toCallback = (cb) => (s) =>
  s.consume((err, x, push, next) => {
    if (err) {
      cb(err);
    } else if (x === _.nil) {
      cb(null, 'Success');
    } else {
      next();
    }
  })
    .resume();

export const toPromise = (s) => {
  if (options().metrics) {
    return options().metrics.toPromise(s);
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
