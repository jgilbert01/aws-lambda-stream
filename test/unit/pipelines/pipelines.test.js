import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize, initializeFrom } from '../../../src/pipelines';
import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import { FAULT_EVENT_TYPE } from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import Connector from '../../../src/connectors/eventbridge';

describe('pipelines/index.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
    delete process.env.DISABLED_PIPELINES;
  });
  afterEach(sinon.restore);

  it('should invoke all pipelines', (done) => {
    let counter = 0;

    const count = (uow) => {
      uow.counter = counter++; // eslint-disable-line no-plusplus
      return uow;
    };

    const events = toKinesisRecords([{
      type: 't1',
    }]);

    initialize({
      p1: (opt) => (s) => s
        // .tap(() => console.log('opt: %s', opt))
        // .tap(console.log)
        .map(count),
      p2: (opt) => (s) => s.map(count),
      p3: (opt) => (s) => s.map(count),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(3);
        expect(counter).to.equal(3);
      })
      .done(done);
  });

  it('should ignore disabled pipelines - string', (done) => {
    process.env.DISABLED_PIPELINES = 'p1a';
    let counter = 0;

    const count = (uow) => {
      uow.counter = counter++; // eslint-disable-line no-plusplus
      return uow;
    };

    const events = toKinesisRecords([{
      type: 't1',
    }]);

    initialize({
      p1: (opt) => (s) => s.map(count),
      p1a: (opt) => (s) => s.map(count),
      p1b: (opt) => (s) => s.map(count),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(2);
        expect(counter).to.equal(2);
        expect(collected[0].pipeline).to.equal('p1b');
        expect(collected[1].pipeline).to.equal('p1');
      })
      .done(done);
  });

  it('should ignore disabled pipelines - array', (done) => {
    process.env.DISABLED_PIPELINES = ['p1', 'p2'];
    let counter = 0;

    const count = (uow) => {
      uow.counter = counter++; // eslint-disable-line no-plusplus
      return uow;
    };

    const events = toKinesisRecords([{
      type: 't1',
    }]);

    initialize({
      p1: (opt) => (s) => s.map(count),
      p2: (opt) => (s) => s.map(count),
      p3: (opt) => (s) => s.map(count),
    })
      .assemble(fromKinesis(events), false)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(1);
        expect(counter).to.equal(1);
        expect(collected[0].pipeline).to.equal('p3');
      })
      .done(done);
  });

  it('should propagate pipeline errors', (done) => {
    const events = toKinesisRecords([{
      type: 't2',
    }]);

    initialize({
      px1: (opt) => (s) => s
        .map((uow) => {
          const e = Error('simulated error');
          e.uow = uow;
          throw e;
        })
        .map(expect.fail),
    }, defaultOptions)
      .assemble(fromKinesis(events))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0].event.type).to.equal(FAULT_EVENT_TYPE);
        expect(collected[0].event.err.name).to.equal('Error');
        expect(collected[0].event.err.message).to.equal('simulated error');

        expect(collected[0].event.tags.functionname).to.equal('undefined');
        expect(collected[0].event.tags.pipeline).to.equal('px1');

        expect(collected[0].uow).to.be.not.null;
      })
      .done(done);
  });

  it('should propagate head errors', (done) => {
    const events = toKinesisRecords([{
      type: 't3',
    }]);

    const head = fromKinesis(events)
      .map((uow) => {
        const e = Error('simulated head error');
        e.uow = uow;
        throw e;
      })
      .map(expect.fail);

    initialize({
      px2: (opt) => (s) => s,
    }, defaultOptions)
      .assemble(head, true)
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0].event.type).to.equal(FAULT_EVENT_TYPE);
        expect(collected[0].event.err.name).to.equal('Error');
        expect(collected[0].event.err.message).to.equal('simulated head error');

        expect(collected[0].event.tags.functionname).to.equal('undefined');
        expect(collected[0].event.tags.pipeline).to.equal('undefined');

        expect(collected[0].uow).to.be.not.null;
      })
      .done(done);
  });

  it('should propagate unhandled head error', (done) => {
    const spy = sinon.spy();
    const err = new Error('unhandled head error');

    const events = toKinesisRecords([{
      type: 't4',
    }]);

    const head = fromKinesis(events)
      .map((uow) => {
        throw err;
      })
      .map(expect.fail);

    initialize({
      px3: (opt) => (s) => s,
      px4: (opt) => (s) => s,
    }, defaultOptions)
      .assemble(head, true)
      .map(expect.fail)
      .stopOnError(spy)
      .collect()
      .tap((collected) => {
        expect(spy).to.have.been.calledWith(err);
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });

  it('should initialize from rules', () => {
    const pipelines = initializeFrom([
      {
        id: 'px5',
        flavor: (rule) => (s) => s.map(rule.map(rule)),
        map: (rule) => (uow) => ({ ...uow, v: rule.value }),
        value: 1,
      },
      {
        id: 'px6',
        flavor: (rule) => (s) => s,
      },
    ]);

    initialize(pipelines);

    // console.log(pipelines);

    expect(Object.keys(pipelines)).to.deep.equal(['px5', 'px6']);
    expect(pipelines.px5).to.be.a('function');
    expect(pipelines.px6).to.be.a('function');
  });
});
