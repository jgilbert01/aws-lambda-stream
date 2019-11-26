export const debug = require('debug'); // eslint-disable-line global-require

export const printStartPipeline = (id) => {
  const d = debug(id);

  return (uow) => {
    const { event } = uow;

    d('start type: %s, eid: %s', event.type, event.id);
  };
};

export const printEndPipeline = (id) => {
  const d = debug(id);

  return (uow) => {
    const { event } = uow;

    d('end type: %s, eid: %s, uow: %j', event.type, event.id, uow);
  };
};
