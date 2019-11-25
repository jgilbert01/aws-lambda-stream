export const debug = require('debug'); // eslint-disable-line global-require

export const now = () => Date.now();

export * from './dynamodb';
export * from './kinesis';
export * from './faults';
export * from './handler';
