import isFunction from 'lodash/isFunction';

export const now = () => Date.now();

export const ttl = (start, days) => Math.floor(start / 1000) + (60 * 60 * 24 * days);

export const ttlRule = (rule, uow) => {
  if (isFunction(rule.ttl)) {
    return rule.ttl(rule, uow);
  } else {
    return ttl(uow.event.timestamp, rule.ttl || rule.defaultTtl || process.env.TTL || 33);
  }
};
