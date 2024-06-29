import _ from 'highland';
import memoryCache from 'memory-cache';

import { faults, flushFaults } from '../faults';

import {
  debug as d, encryptData, decryptData, options,
} from '../utils';

const debug = d('pl:init');

let thePipelines = {};

export const initialize = (pipelines, opt = {}) => {
  const keys = Object.keys(pipelines);
  options(opt);

  debug('initialize: %j', keys);

  thePipelines = keys.reduce(
    (accumulator, id) => ({
      ...accumulator,
      [id]: pipelines[id]({ // pass in options
        id,
        ...opt,
        ...addDebug(id),
        ...addEncryptors(opt),
      }),
    }),
    {},
  );

  memoryCache.clear();

  return { assemble: assemble(opt) };
};

export const initializeFrom = (rules) => rules.reduce(
  (accumulator, rule) => ({
    ...accumulator,
    [rule.id]: (opt) => (rule.pattern || rule.flavor)({
      ...rule, // included 1st so rules are printed 1st in debug output
      ...opt,
      ...rule, // include again for override precedence
      ...addEncryptors({ ...opt, ...rule }),
    }),
  }),
  {},
);

const assemble = (opt) => (head, includeFaultHandler = true) => {
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
        .map(initPipeline(p.id))
        .tap(startPipeline(opt))
        .through(p)
        .through(endPipeline(opt, p.id));
    });

    debug('FORK: %s', lines[last].id);
    const p = lines[last];
    lines[last] = head
      .map(initPipeline(p.id))
      .tap(startPipeline(opt, keys.length))
      .through(lines[last])
      .through(endPipeline(opt, p.id));
  }

  let s = _(lines).merge();

  if (includeFaultHandler) {
    s = s.errors(faults)
      .through(flushFaults({
        ...opt,
        ...addDebug('fault'),
      }));
  }

  return s;
};

const addDebug = (id) => ({ debug: d(`pl:${id}`) });

const initPipeline = (pipeline) => (uow) => ({
  // shallow clone of data per pipeline
  pipeline,
  ...uow,
  ...addDebug(pipeline),
});

const startPipeline = (opt, pipelineCount) => (uow) => {
  if (opt.metrics) {
    uow.metrics = uow.metrics.startPipeline(uow, pipelineCount, opt);
  }
};

const endPipeline = (opt, pipelineId) => (s) =>
  (opt.metrics ? opt.metrics.endPipeline(pipelineId, opt, s) : s);

const addEncryptors = (opt) => ({
  encrypt: encryptData(opt),
  decrypt: decryptData(opt),
});
