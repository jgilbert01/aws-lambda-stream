import * as pipelines from './pipelines';
import * as capture from './capture';
import * as toPromise from './to-promise';

const enabled = (key) => process.env.METRICS.includes(key) || process.env.METRICS.includes('*');

// handler middleware
export const metrics = (next, opt, evt, ctx) => {
  /* istanbul ignore else */
  if (process.env.METRICS?.length) {
    opt.metrics = {
      ...pipelines,
      ...capture,
      ...toPromise,
      enabled,
    };

    /* istanbul ignore else */
    if (process.env.METRICS.includes('xray') && process.env.AWS_XRAY_DAEMON_ADDRESS) {
      opt.xrayEnabled = true;
    }
  }

  // could collect metrics here
  return next();
  // could collect metrics here in .tap() and .tapCatch()
};
