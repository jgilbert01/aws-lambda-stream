import { publishToEventBridge } from './eventbridge';

export const defaultOptions = { // eslint-disable-line import/prefer-default-export
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,
};
