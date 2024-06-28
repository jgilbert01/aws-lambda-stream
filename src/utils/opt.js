import { publishToEventBridge } from '../sinks/eventbridge';

import { debug } from './print';
import * as metrics from '../metrics';

export const defaultOptions = { // eslint-disable-line import/prefer-default-export
  debug: debug('handler'),
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,

  // TODO opt.metricsEnabled levels 'essential, detailed, etc'
  metrics: process.env.ENABLE_METRICS === 'true' ? /* istanbul ignore next */ metrics : undefined,
  xrayEnabled: process.env.XRAY_ENABLED === 'true' || process.env.AWS_XRAY_DAEMON_ADDRESS,

  maxRequestSize: Number(process.env.MAX_REQ_SIZE) || 1024 * 256, // 262,144
  compressionThreshold: Number(process.env.COMPRESSION_THRESHOLD) || 1024 * 10,

  // encryption
  eemField: 'eem',
  masterKeyAlias: process.env.MASTER_KEY_ALIAS,
  regions: (process.env.KMS_REGIONS && process.env.KMS_REGIONS.split(',')),
  AES: process.env.AES !== 'false',
};
