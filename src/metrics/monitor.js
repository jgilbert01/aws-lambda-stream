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

  // could collect metrics here
  return (event, context) => Promise.resolve().then(() => handle(event, context));
  // could collect metrics here in .tap() and .tapCatch()
};
