import _ from 'highland';
import Promise from 'bluebird';

import { clear } from './pipelines';
import { calculateMetrics } from './calculate';

export const toPromise = (opt, s) => // eslint-disable-line import/prefer-default-export
  new Promise((resolve, reject) => {
    clear(opt);
    const collected = [];
    s.consume((err, x, push, next) => {
      /* istanbul ignore if */
      if (err) {
        reject(err);
      } else if (x === _.nil) {
        const metrics = calculateMetrics(collected);
        // TODO logMetrics(metrics);
        resolve(metrics);
      } else {
        collected.push(x);
        next();
      }
    })
      .resume();
  });
