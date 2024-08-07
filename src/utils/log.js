/* eslint-disable import/prefer-default-export */
import { debug as d } from 'debug';
import { inspect } from 'util';

export const defaultDebugLogger = (debug) => {
  const dbg = debug || /* istanbul ignore next */ d('debug');

  const log = (...content) => dbg(...(content.map(normalizeLogMsg)));
  return {
    debug: () => {},
    info: log,
    warn: log,
    error: log,
  };
};

export const normalizeLogMsg = (msg) => (typeof msg === 'string' ? msg : inspect(msg, { depth: 3 })).replace(/\n/g, '\r');
