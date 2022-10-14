import {
  initialize,
  initializeFrom,
  defaultOptions,
  decryptEvent,
  fromKinesis,
  prefilterOnEventTypes,
  toPromise,
} from 'aws-lambda-stream';

import RULES from './rules';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  ...initializeFrom(RULES),
};

const { debug } = OPTIONS;

export class Handler {
  constructor(options = OPTIONS) {
    this.options = options;
  }

  handle(event, includeErrors = true) {
    return initialize(PIPELINES, this.options)
      .assemble(fromKinesis(event)
        .through(decryptEvent({
          ...this.options,
          prefilter: prefilterOnEventTypes(RULES),
        })),
      includeErrors);
  }
}

export const handle = async (event, context, int = {}) => {
  debug('event: %j', event);
  debug('context: %j', context);

  return new Handler({ ...OPTIONS, ...int })
    .handle(event)
    .through(toPromise);
};
