import isFunction from 'lodash/isFunction';
import get from 'lodash/get';

import { faulty } from './faults';

export const splitObject = ({ // eslint-disable-line import/prefer-default-export
  splitOn,
  splitTargetField = 'split',
  ...rule
}) => {
  if (splitOn) {
    if (isFunction(splitOn)) {
      return (s) => s
        .flatMap(faulty((uow) => splitOn(uow, rule)));
    } else {
      return (s) => s
        .flatMap(faulty((uow) => {
          const values = get(uow.event, splitOn);
          return values.map((v) => ({
            ...uow,
            [splitTargetField]: v,
          }));
        }));
    }
  } else {
    return (s) => s;
  }
};
