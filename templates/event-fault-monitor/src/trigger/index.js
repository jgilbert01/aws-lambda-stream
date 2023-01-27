import {
  initialize,
  defaultOptions,
  fromSqsSnsS3,
  toPromise,
} from 'aws-lambda-stream';

// import alert from './alert';
import alert from './dd-alert';
import getFaults from './get-faults';
import metrics from './metrics';

const OPTIONS = { ...defaultOptions };

const PIPELINES = {
  metrics,
  alert,
};

const { debug } = OPTIONS;

export class Handler {
  handle(event, includeErrors = false) {
    return initialize(PIPELINES, OPTIONS)
      .assemble(fromSqsSnsS3(event)
        .through(getFaults(OPTIONS)), includeErrors);
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
