import {
  initialize,
  initializeFrom,
  defaultOptions,
  fromKinesis,
  toPromise,
  debug as d,
} from 'aws-lambda-stream';

import RULES from './collect-rules';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  ...initializeFrom(RULES),
};

const debug = d('handler');

export class Handler {
  handle(event, includeErrors = true) {
    return initialize(PIPELINES, OPTIONS)
      .assemble(fromKinesis(event), includeErrors);
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
