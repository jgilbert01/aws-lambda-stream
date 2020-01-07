import _ from 'highland';

import { faults, flushFaults } from '../faults';

import { debug as d } from '../utils';

const debug = d('pl:init');

let thePipelines = {};

export const initialize = (pipelines, opt = {}) => {
  const keys = Object.keys(pipelines);

  debug('initialize: %j', keys);

  thePipelines = keys.reduce(
    (accumulator, id) => ({
      ...accumulator,
      [id]: pipelines[id]({ // pass in options
        id,
        ...opt,
        ...addDebug(id),
      }),
    }),
    {},
  );

  return { assemble };
};

export const initializeFrom = (rules) => rules.reduce(
  (accumulator, rule) => ({
    ...accumulator,
    [rule.id]: (opt) => rule.pipeline({
      ...rule, // included 1st so rules are printed 1st in debug output
      ...opt,
      ...rule, // include again for override precedence
    }),
  }),
  {},
);

export const assemble = (head, includeFaultHandler = true) => {
  const keys = Object.keys(thePipelines);

  debug('assemble: %j', keys);

  if (includeFaultHandler) {
    // after pre processoring
    head = head.errors(faults);
  }

  const lines = keys.map((key) => {
    const f = thePipelines[key];
    const p = _.pipeline(f);
    p.id = key;
    return p;
  });

  /* istanbul ignore else */
  if (lines.length > 0) {
    const last = lines.length - 1;

    lines.slice(0, last).forEach((p, i) => {
      debug('FORK: %s', p.id);
      const os = head.observe();

      lines[i] = os
        // shallow clone of data per pipeline
        .map((uow) => ({
          pipeline: p.id,
          ...uow,
          ...addDebug(p.id),
        }))
        .through(p);
    });

    debug('FORK: %s', lines[last].id);
    const p = lines[last];
    lines[last] = head
      .map((uow) => ({
        pipeline: p.id,
        ...uow,
        ...addDebug(p.id),
      }))
      .through(lines[last]);
  }

  let s = _(lines).merge();

  if (includeFaultHandler) {
    s = s.errors(faults)
      .through(flushFaults);
  }

  return s;
};

const addDebug = (id) => ({ debug: d(`pl:${id}`) });
