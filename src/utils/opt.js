import { publishToEventBridge } from './eventbridge';

import { debug } from './print';

export const defaultOptions = { // eslint-disable-line import/prefer-default-export
  debug: debug('handler'),
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,
};
