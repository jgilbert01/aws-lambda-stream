import _ from 'highland';
import Promise from 'bluebird';

import * as pipelines from './pipelines';
import * as capture from './capture';
import { calculateMetrics } from './calculate';
import { logMetrics } from './emf';

export const toPromise = (opt, s) =>
  new Promise((resolve, reject) => {
    pipelines.clear(opt);
    const collected = [];
    s.consume((err, x, push, next) => {
      /* istanbul ignore if */
      if (err) {
        reject(err);
      } else if (x === _.nil) {
        const metrics = calculateMetrics(collected);
        // logMetrics(metrics);
        resolve(metrics);
      } else {
        collected.push(x);
        next();
      }
    })
      .resume();
  });

// handler middleware
export const metrics = (next, opt, evt, ctx) => {
  /* istanbul ignore else */
  if (process.env.ENABLE_METRICS === 'true') {
    opt.metrics = {
      ...pipelines,
      ...capture,
      toPromise,
    };
  }

  /* istanbul ignore else */
  if (process.env.ENABLE_XRAY === 'true' || process.env.AWS_XRAY_DAEMON_ADDRESS) {
    opt.xrayEnabled = true;
  }

  // could collect metrics here
  return next();
  // could collect metrics here in .tap() and .tapCatch()
};
