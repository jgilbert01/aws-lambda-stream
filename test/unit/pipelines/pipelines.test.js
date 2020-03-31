import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { initialize, assemble, initializeFrom } from '../../../src/pipelines';
import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import { FAULT_EVENT_TYPE } from '../../../src';

import Connector from '../../../src/connectors/kinesis';

describe('pipelines/index.js', () => {
  beforeEach(() => {
    sinon.stub(Connector.prototype, 'putRecords').resolves({});
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

  it('should propagate pipeline errors', (done) => {
    initialize({
      px1: (opt) => (s) => s
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

    assemble(fromKinesis(events))
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
    initialize({
      px2: (opt) => (s) => s,
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

    assemble(head, true)
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

    initialize({
      px3: (opt) => (s) => s,
      px4: (opt) => (s) => s,
    });

    const events = toKinesisRecords([{
      type: 't4',
    }]);

    const head = fromKinesis(events)
      .map((uow) => {
        throw err;
      })
      .map(expect.fail);

    assemble(head, true)
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
