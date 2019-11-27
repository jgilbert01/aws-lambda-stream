export const debug = require('debug'); // eslint-disable-line global-require

export const printStartPipeline = (uow) => {
  printStart(uow.debug)(uow);
};

export const printEndPipeline = (uow) => {
  printEnd(uow.debug)(uow);
};

export const printStart = (dbug) => (uow) => {
  dbug('start type: %s, eid: %s', uow.event.type, uow.event.id);
};

export const printEnd = (dbug) => (uow) => {
  dbug('end type: %s, eid: %s, uow: %j', uow.event.type, uow.event.id, uow);
};
