import _ from 'highland';

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

export const toPromise = (s) =>
  new Promise((resolve, reject) => {
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
