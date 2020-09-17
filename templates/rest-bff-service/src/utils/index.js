export const debug = require('debug')('handler');

export const now = () => Date.now();

export const ttl = (start, days) => Math.floor(start / 1000) + (60 * 60 * 24 * days);
