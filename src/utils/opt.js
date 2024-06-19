import { isServerError, isThrottlingError, isTransientError } from '@smithy/service-error-classification';

import { publishToEventBridge } from '../sinks/eventbridge';

import { debug } from './print';

export const retryable = (err, opt) => (isThrottlingError(err) || isTransientError(err) || isServerError(err));

export const defaultOptions = { // eslint-disable-line import/prefer-default-export
  debug: debug('handler'),
  busName: process.env.BUS_NAME,
  publish: publishToEventBridge,

  metricsEnabled: process.env.ENABLE_METRICS === 'true',

  maxRequestSize: Number(process.env.MAX_REQ_SIZE) || 1024 * 256, // 262,144
  compressionThreshold: Number(process.env.COMPRESSION_THRESHOLD) || 1024 * 10,

  // encryption
  eemField: 'eem',
  masterKeyAlias: process.env.MASTER_KEY_ALIAS,
  regions: (process.env.KMS_REGIONS && process.env.KMS_REGIONS.split(',')),
  AES: process.env.AES !== 'false',

  // lambda retry
  // can disable in index file by setting to undefined
  retryable,
};
