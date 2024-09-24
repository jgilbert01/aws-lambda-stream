import isArray from 'lodash/isArray';
import isFunction from 'lodash/isFunction';

export const filterOnEventType = (rule, uow) => {
  uow.debug && uow.debug('filterOnEventType: ', rule.eventType, uow.event.type);

  if (typeof rule.eventType === 'string') {
    return uow.event.type === rule.eventType;
  } else if (rule.eventType instanceof RegExp) {
    return rule.eventType.test(uow.event.type);
  } else if (isArray(rule.eventType)) {
    return rule.eventType.includes(uow.event.type);
  } else if (isFunction(rule.eventType)) {
    return rule.eventType(uow.event.type, rule);
  } else {
    throw new Error(`Rule: ${rule.id}, has improperly configured eventType filter. Must be a string, array of string, regex or function.`);
  }
};

export const prefilterOnEventTypes = (rules) =>
  (uow) =>
    rules.reduce((a, c) => a || filterOnEventType(c, uow), false);

export const filterOnContent = (rule, uow) => {
  /* istanbul ignore else */
  if (rule.filters) {
    return rule.filters.reduce((a, c) => a && c(uow, rule), true);
  } else {
    return true;
  }
};
