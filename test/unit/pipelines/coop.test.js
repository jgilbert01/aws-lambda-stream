import 'mocha';

import _ from 'highland';
import Promise from 'bluebird';

import { initialize, execute } from '../../../src/pipelines';

import {
  fromKinesis, toKinesisRecords, now,
} from '../../../src';

describe.skip('pipelines/coop-example', () => {
  it('should show pipeline cooperation', (done) => {
    const FACTOR = 1;
    const PARALLEL = 2;
    const BATCH_SIZE = [10 * FACTOR, 5 * FACTOR, 3 * FACTOR];

    const events = generate(100 * FACTOR);

    initialize({
      odd: (opt) => (s) => s
        .filter(filterOdd)
        .map(mapOdd)
        .batch(BATCH_SIZE[0])
        .map(daoActionOdd)
        .parallel(PARALLEL),

      even: (opt) => (s) => s
        .filter(filterEven)
        .map(mapEven)
        .batch(BATCH_SIZE[1])
        .map(daoActionEven)
        .parallel(PARALLEL),

      all: (opt) => (s) => s
        .map(mapAll)
        .batch(BATCH_SIZE[2])
        .map(daoActionAll)
        .parallel(PARALLEL),

    });

    execute(fromKinesis(events))
      // .stopOnError(console.error)
      .collect()
      .tap((collected) => {
        debug(`DONE: ${collected.length}`);
        // console.log(JSON.stringify(collected, null, 2));
      })
      .done(done);
  });
});

const debug = require('debug')('coop');

let counter = 0;

const filter = (name, rh, uow) => {
  debug('%s FILTER: %d', name, uow.event.i);
  return uow.event.i % 2 === rh;
};

const filterEven = (uow) => filter('EVEN', 0, uow);
const filterOdd = (uow) => filter('ODD', 1, uow);

const map = (name, uow) => {
  debug('%s MAP: %d', name, uow.event.i);
  uow.counter = counter++; // eslint-disable-line no-plusplus
  return uow;
};

const mapAll = (uow) => map('ALL', uow);
const mapEven = (uow) => map('EVEN', uow);
const mapOdd = (uow) => map('ODD', uow);

const writeFile = Promise.promisify(require('fs').writeFile);

const daoAction = (name, uow) => {
  debug('%s SINK: %d ----------', name, uow.length);
  const time = now();
  const fileName = `./.log/pipeline-${name}-${time}.json`;
  const p = writeFile(fileName, JSON.stringify(uow));
  return _(p.then(() => uow)).sequence();
};

const daoActionAll = (uow) => daoAction('ALL', uow);
const daoActionEven = (uow) => daoAction('EVEN', uow);
const daoActionOdd = (uow) => daoAction('ODD', uow);

const TYPES = ['c1', 'c2', 'c3', 'c4'];

const generate = (count) => {
  const events = [];
  for (let i = 0; i < count; i++) { // eslint-disable-line no-plusplus
    events.push({
      type: TYPES[Math.floor(Math.random() * TYPES.length)],
      timestamp: now(),
      i,
    });
  }

  return toKinesisRecords(events);
};
