import {
  initialize,
  defaultOptions,
  fromS3,
  toPromise,
} from 'aws-lambda-stream';

import alert from './alert';
import getFaults from './get-faults';
import metrics from './metrics';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  metrics,
  alert,
};

const { debug } = OPTIONS;

export class Handler {
  handle(event, includeErrors = true) {
    return initialize(PIPELINES, OPTIONS)
      .assemble(fromS3(event)
        .through(getFaults(OPTIONS)), includeErrors);
  }
}

export const handle = async (event, context) => {
  debug('event: %j', event);
  debug('context: %j', context);

  return new Handler()
    .handle(event)
    .through(toPromise);
};
