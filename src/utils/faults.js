import _ from 'highland';
import Promise from 'bluebird';

export const throwFault = (uow, ignore = false) => (err) => {
  /* istanbul ignore else */
  if (!ignore) {
    // adorn the troubled uow
    // for processing in the errors handler
    err.uow = uow;
  }
  throw err;
};

export const rejectWithFault = (uow, ignore = false) => (err) => {
  /* istanbul ignore else */
  if (!ignore) {
    // adorn the troubled uow
    // for processing in the errors handler
    err.uow = uow;
  }
  return Promise.reject(err);
};

export const faulty = (funct) => (uow) => { // eslint-disable-line consistent-return
  try {
    return funct(uow);
  } catch (e) {
    throwFault(uow)(e);
  }
};

export const faultyAsync = (funct) => (uow) =>
  _(
    funct(uow)
      .catch(rejectWithFault(uow)),
  );

export const toResolve = (func, uow, rule) => new Promise((resolve, reject) => {
  try {
    resolve(func(uow, rule));
  } catch (e) {
    reject(e);
  }
});
