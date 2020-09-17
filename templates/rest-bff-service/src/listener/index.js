import {
  initialize,
  initializeFrom,
  defaultOptions,
  fromKinesis,
  toPromise,
} from 'aws-lambda-stream';

import { debug } from '../utils';

import RULES from './rules';

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
