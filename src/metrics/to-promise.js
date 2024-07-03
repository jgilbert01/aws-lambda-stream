import _ from 'highland';
import Promise from 'bluebird';

import { clear } from './pipelines';
import { calculateMetrics } from './calculate';
// import { logMetrics } from './emf';

export const toPromise = (opt, s) =>
  new Promise((resolve, reject) => {
    clear(opt);
    const collected = [];
    s.consume((err, x, push, next) => {
      /* istanbul ignore if */
      if (err) {
        reject(err);
      } else if (x === _.nil) {
        const metrics = calculateMetrics(collected);
        // logMetrics(metrics);
        resolve(metrics); //  TODO limit metrics returned ???
      } else {
        collected.push(x);
        next();
      }
    })
      .resume();
  });
