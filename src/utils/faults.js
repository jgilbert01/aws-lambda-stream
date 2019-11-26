import _ from 'highland';
import Promise from 'bluebird';

export const throwFault = (uow) => (err) => {
  // adorn the troubled uow
  // for processing in the errors handler
  err.uow = uow;
  throw err;
};

export const rejectWithFault = (uow) => (err) => {
  // adorn the troubled uow
  // for processing in the errors handler
  err.uow = uow;
  return Promise.reject(err);
};

export const faulty = (funct) => (uow) => { // eslint-disable-line consistent-return
  try {
    return funct(uow);
  } catch (e) {
    throwFault(uow)(e);
  }
};

export const faultyAsync = (funct) => (uow) => // eslint-disable-line consistent-return
  _(
    funct(uow)
      .catch(rejectWithFault),
  );
