import {
  initialize,
  defaultOptions,
  fromS3,
  toPromise,
} from 'aws-lambda-stream';

import alert from './alert';
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
      .assemble(fromS3(event), includeErrors);
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
