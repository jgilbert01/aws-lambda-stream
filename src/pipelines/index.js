import _ from 'highland';
import memoryCache from 'memory-cache';

import { faults, flushFaults } from '../faults';

import { debug as d, encryptData, decryptData } from '../utils';

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
  const xrayEnabled = Boolean(opt.xrayEnabled);
  if (xrayEnabled) require('../utils/xray').clearPipelineSegments();
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
        .map(startSegment(xrayEnabled, p.id))
        .through(p)
        .through(endSegment(xrayEnabled, p.id));
    });

    debug('FORK: %s', lines[last].id);
    const p = lines[last];
    lines[last] = head
      .map((uow) => ({
        pipeline: p.id,
        ...uow,
        ...addDebug(p.id),
      }))
      .map(startSegment(xrayEnabled, p.id))
      .through(lines[last])
      .through(endSegment(xrayEnabled, p.id));
  }

  let s = _(lines).merge();

  if (includeFaultHandler) {
    s = s.errors(faults)
      .map(startSegment(xrayEnabled, 'FlushFaults'))
      .through(flushFaults({
        ...opt,
        ...addDebug('fault'),
      }))
      .through(endSegment(xrayEnabled, 'FlushFaults'));
  }

  return s;
};

const addDebug = (id) => ({ debug: d(`pl:${id}`) });

const startSegment = (xrayEnabled, pipelineId) =>
  (xrayEnabled ? require('../utils/xray').startPipelineSegment(pipelineId) : (uow) => uow);

const endSegment = (xrayEnabled, pipelineId) => (s) =>
  (xrayEnabled ? s.through(require('../utils/xray').terminateSegment(pipelineId)) : s);

const addEncryptors = (opt) => ({
  encrypt: encryptData(opt),
  decrypt: decryptData(opt),
});
