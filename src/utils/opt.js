import { publishToEventBridge } from './eventbridge';

export default {
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,
};
