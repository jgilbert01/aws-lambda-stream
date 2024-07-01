import { publishToEventBridge } from '../sinks/eventbridge';

import { debug } from './print';

let opt = {};
export const options = (o) => {
  if (o) opt = o;
  return opt;
};

export const defaultOptions = {
  debug: debug('handler'),
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,

  maxRequestSize: Number(process.env.MAX_REQ_SIZE) || 1024 * 256, // 262,144
  compressionThreshold: Number(process.env.COMPRESSION_THRESHOLD) || 1024 * 10,

  // encryption
  eemField: 'eem',
  masterKeyAlias: process.env.MASTER_KEY_ALIAS,
  regions: (process.env.KMS_REGIONS && process.env.KMS_REGIONS.split(',')),
  AES: process.env.AES !== 'false',
};
