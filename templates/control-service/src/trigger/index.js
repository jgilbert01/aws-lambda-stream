import {
  initialize,
  initializeFrom,
  defaultOptions,
  fromDynamodb,
  toPromise,
  debug as d,
  expired,
} from 'aws-lambda-stream';

import CORRELATE_RULES from './correlate-rules';
import EVAL_RULES from './evaluate-rules';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  ...initializeFrom(CORRELATE_RULES),
  ...initializeFrom(EVAL_RULES),
  expired,
};

const debug = d('handler');

export class Handler {
  handle(event, includeErrors = true) {
    return initialize(PIPELINES, OPTIONS)
      .assemble(fromDynamodb(event), includeErrors);
  }
}

export const handle = async (event, context) => {
  debug('event: %j', event);
  debug('context: %j', context);

  return new Handler()
    .handle(event)
    .tap(debug)
    .through(toPromise);
};
