//----------------------------
// N-directional sync support
//----------------------------

// use this filter in listeners to not consume events my service just emitted
export const outSourceIsSelf = (uow) => !uow.event.tags || uow.event.tags.source !== (process.env.PROJECT || /* istanbul ignore next */ process.env.SERVERLESS_PROJECT);

// use this filter in triggers to not emit events in reaction to an update from a listener
// listeners should set latched = true
// commands/mutations should set latched = null
export const outLatched = (uow) =>
  // create & update latch
  (uow.event.raw.new && !uow.event.raw.new.latched)
    // delete latch
    || (!uow.event.raw.new && uow.event.raw.old && !uow.event.raw.old.latched);
