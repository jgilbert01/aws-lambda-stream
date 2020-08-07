import {
  initialize,
  initializeFrom,
  defaultOptions,
  fromKinesis,
  toPromise,
  debug,
} from 'aws-lambda-stream';

import RULES from './collect-rules';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  ...initializeFrom(RULES),
};

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
