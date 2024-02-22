import isArray from 'lodash/isArray';
import isFunction from 'lodash/isFunction';

export const filterOnEventType = (rule, uow) => {
  if (typeof rule.eventType === 'string') {
    return uow.event.type === rule.eventType;
  } else if (rule.eventType instanceof RegExp) {
    return rule.eventType.test(uow.event.type);
  } else if (isArray(rule.eventType)) {
    return rule.eventType.join().indexOf(uow.event.type) > -1;
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
    const isSuccess = rule.filters.reduce((a, c) => a && c(uow, rule), true);
    // Invoke custom function incase of filter conditions are not statisfied
    // For example to create cloudwatch metric in case of failure
    if (!isSuccess && rule.errorFilter) {
      rule.errorFilter(uow);
    }
    return isSuccess;
  } else {
    return true;
  }
};

export * from './latch';
export * from './skip';
