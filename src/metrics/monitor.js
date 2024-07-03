import Promise from 'bluebird';

import * as pipelines from './pipelines';
import * as capture from './capture';
import * as toPromise from './to-promise';

export const monitor = (handle, opt) => { // eslint-disable-line import/prefer-default-export
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
  return (event, context, int) => Promise.resolve().then(() => handle(event, context, int));
  // could collect metrics here in .tap() and .tapCatch()
};
