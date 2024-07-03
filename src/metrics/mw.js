import * as pipelines from './pipelines';
import * as capture from './capture';
import * as toPromise from './to-promise';

// handler middleware
export const metrics = (next, opt, evt, ctx) => {
  /* istanbul ignore else */
  if (process.env.ENABLE_METRICS === 'true') {
    opt.metrics = {
      ...pipelines,
      ...capture,
      ...toPromise,
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
