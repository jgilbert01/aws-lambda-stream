import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize, execute, initializeFrom } from '../../../src/pipelines';
import {
  fromKinesis, Publisher, toKinesisRecords, FAULT_EVENT_TYPE,
} from '../../../src';

describe('pipelines/index.js', () => {
  beforeEach(() => {
    sinon.stub(Publisher.prototype, 'publish').resolves({});
  });
  afterEach(sinon.restore);

  it('should invoke all pipelines', (done) => {
    let counter = 0;

    const count = (uow) => {
      uow.counter = counter++; // eslint-disable-line no-plusplus
      return uow;
    };

    initialize({
      p1: (s) => s
        // .tap(console.log)
        .map(count),
      p2: (s) => s.map(count),
      p3: (s) => s.map(count),
    });

    const events = toKinesisRecords([{
      type: 't1',
    }]);

    execute(fromKinesis(events), false)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(3);
        expect(counter).to.equal(3);
      })
      .done(done);
  });

  it('should propagate pipeline errors', (done) => {
    initialize({
      px1: (s) => s
        .map((uow) => {
          const e = Error('simulated error');
          e.uow = uow;
          throw e;
        })
        .map(expect.fail),
    });

    const events = toKinesisRecords([{
      type: 't2',
    }]);

    execute(fromKinesis(events))
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0].type).to.equal(FAULT_EVENT_TYPE);
        expect(collected[0].err.name).to.equal('Error');
        expect(collected[0].err.message).to.equal('simulated error');

        expect(collected[0].tags).to.deep.equal({
          functionname: 'undefined',
          pipeline: 'px1',
        });

        expect(collected[0].uow).to.be.not.null;
      })
      .done(done);
  });

  it('should propagate head errors', (done) => {
    initialize({
      px2: (s) => s,
    });

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

    execute(head, true)
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(1);

        expect(collected[0].type).to.equal(FAULT_EVENT_TYPE);
        expect(collected[0].err.name).to.equal('Error');
        expect(collected[0].err.message).to.equal('simulated head error');

        expect(collected[0].tags).to.deep.equal({
          functionname: 'undefined',
          pipeline: 'undefined',
        });

        expect(collected[0].uow).to.be.not.null;
      })
      .done(done);
  });

  it('should propagate unhandled head error', (done) => {
    const spy = sinon.spy();
    const err = new Error('unhandled head error');

    initialize({
      px3: (s) => s,
      px4: (s) => s,
    });

    const events = toKinesisRecords([{
      type: 't4',
    }]);

    const head = fromKinesis(events)
      .map((uow) => {
        throw err;
      })
      .map(expect.fail);

    execute(head, true)
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
        pipeline: (rule) => (s) => s.map(rule.map(rule)),
        map: (rule) => (uow) => ({ ...uow, v: rule.value }),
        value: 1,
      },
      {
        id: 'px6',
        pipeline: (rule) => (s) => s,
      },
    ]);

    // console.log(pipelines);

    expect(Object.keys(pipelines)).to.deep.equal(['px5', 'px6']);
    expect(pipelines.px5).to.be.a('function');
    expect(pipelines.px6).to.be.a('function');
  });
});
