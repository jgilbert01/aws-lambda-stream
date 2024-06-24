import _ from 'highland';
import memoryCache from 'memory-cache';
import AWSXray from 'aws-xray-sdk-core';

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
  if(xrayEnabled) {
    AWSXray.capturePromise();
    AWSXray.captureHTTPsGlobal(require('https'));
  }
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
        .map(startSegment(xrayEnabled))
        .through(p)
        .map(endSegment);
    });

    debug('FORK: %s', lines[last].id);
    const p = lines[last];
    lines[last] = head
      .map((uow) => ({
        pipeline: p.id,
        ...uow,
        ...addDebug(p.id),
      }))
      .map(startSegment(xrayEnabled))
      .through(lines[last])
      .map(endSegment);
  }

  let s = _(lines).merge();

  if (includeFaultHandler) {
    s = s.errors(faults)
      .map(startFaultSegment(xrayEnabled))
      .through(flushFaults({
        ...opt,
        ...addDebug('fault'),
      }))
      .map(endSegment);
  }

  return s;
};

const addDebug = (id) => ({ debug: d(`pl:${id}`) });

const startSegment = (xrayEnabled) => (uow) => (xrayEnabled ? {
  xraySegment: AWSXray.getSegment().addNewSubsegment(uow.pipeline),
  ...uow,
} : {
  ...uow,
});

const startFaultSegment = (xrayEnabled) => (uow) => (xrayEnabled ? {
  xraySegment: AWSXray.getSegment().addNewSubsegment('FlushFaults'),
  ...uow,
} : {
  ...uow,
});

const endSegment = (uow) => {
  uow.xraySegment?.close();
  return uow;
};

const addEncryptors = (opt) => ({
  encrypt: encryptData(opt),
  decrypt: decryptData(opt),
});
