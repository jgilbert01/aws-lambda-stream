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

export const faulty = (funct, ignore = false) => (uow, ...args) => { // eslint-disable-line consistent-return
  try {
    return funct(uow, ...args);
  } catch (e) {
    throwFault(uow, ignore)(e);
  }
};

export const faultyAsync = (funct, ignore = false) => (uow, ...args) =>
  funct(uow, ...args)
    .catch(rejectWithFault(uow, ignore));

export const faultyAsyncStream = (funct, ignore = false) => (...args) => _(faultyAsync(funct, ignore)(...args));

export const faultify = (func) => (...args) => new Promise((resolve, reject) => {
  try {
    resolve(func(...args));
  } catch (e) {
    reject(e);
  }
});
