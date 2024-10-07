import isFunction from 'lodash/isFunction';
import get from 'lodash/get';
import omit from 'lodash/omit';

import { faulty } from './faults';

export const splitObject = ({ // eslint-disable-line import/prefer-default-export
  splitOn,
  splitTargetField = 'split',
  splitOnOmitFields = [],
  ...rule
}) => {
  if (splitOn) {
    if (isFunction(splitOn)) {
      return (s) => s
        .flatMap(faulty((uow) => splitOn(uow, rule)));
    } else {
      return (s) => s
        .flatMap(faulty((uow) => {
          const values = get(uow, splitOn);
          const valuesLength = values.length;
          return values.map((v, i) => ({
            ...omit(uow, splitOnOmitFields),
            splitOnTotal: valuesLength,
            splitOnItemNumber: i + 1,
            [splitTargetField]: v,
          }));
        }));
    }
  } else {
    return (s) => s;
  }
};
