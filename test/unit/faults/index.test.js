import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import Connector from '../../../src/connectors/eventbridge';

import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import {
  faults, flushFaults, FAULT_EVENT_TYPE,
} from '../../../src/faults';

import { defaultOptions } from '../../../src/utils/opt';

let publishStub;

describe('faults/index.js', () => {
  beforeEach(() => {
    publishStub = sinon.stub(Connector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });

  afterEach(sinon.restore);

  it('should handled logic error', (done) => {
    const simulateHandledError = (uow) => {
      if (uow.record.kinesis.sequenceNumber === '1') {
        const e = new Error('handled error, has uow attached');
        e.uow = uow;
        throw e;
      } else {
        return uow;
      }
    };

    const events = toKinesisRecords([
      {
        type: 'f1',
      },
      {
        type: 'f2',
        entity: {
          f1: 'v1',
          f2: 'v2',
        },
        eem: { fields: ['f2'] },
      },
      {
        type: 'f3',
      },
    ]);

    fromKinesis(events)
      .map((uow) => ({
        ...uow,
        someResponse: {
          f3: Buffer.from('v1'),
          f4: uow.event.eem,
        },
      }))
      .map(simulateHandledError)
      .errors(faults)
      .through(flushFaults(defaultOptions))

      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(publishStub.calledOnce);

        expect(collected.length).to.equal(3);

        expect(collected[2].event.type).to.equal(FAULT_EVENT_TYPE);
        expect(collected[2].event.err.name).to.equal('Error');
        expect(collected[2].event.err.message).to.equal('handled error, has uow attached');

        expect(collected[2].event.tags.functionname).to.equal('undefined');
        expect(collected[2].event.tags.pipeline).to.equal('undefined');

        expect(collected[2].event.uow.pipeline).to.be.undefined;
        expect(collected[2].event.uow.record).to.deep.equal({
          eventSource: 'aws:kinesis',
          eventID: 'shardId-000000000000:1',
          awsRegion: 'us-west-2',
          kinesis: {
            sequenceNumber: '1',
            approximateArrivalTimestamp: undefined,
            data: 'eyJ0eXBlIjoiZjIiLCJlbnRpdHkiOnsiZjEiOiJ2MSIsImYyIjoidjIifSwiZWVtIjp7ImZpZWxkcyI6WyJmMiJdfX0=',
          },
        });
        expect(collected[2].event.uow.event).to.deep.equal({
          id: 'shardId-000000000000:1',
          type: 'f2',
          entity: {
            f1: 'v1',
            f2: '[REDACTED]',
          },
          eem: { fields: ['f2'] },
        });
        expect(collected[2].event.uow.someResponse).to.deep.equal({
          f3: '[BUFFER: 2]',
          f4: '[CIRCULAR]',
        });
      })
      .done(done);
  });

  it('should account for unhandled logic error', (done) => {
    const spy = sinon.spy();
    const err = new Error('unhandled error, no uow attached');
    const simulateUnhandledError = (uow) => {
      throw err;
    };

    const events = toKinesisRecords([
      {
        type: 'u1',
      },
    ]);

    fromKinesis(events)
      .map(simulateUnhandledError)
      .errors(faults)
      .stopOnError(spy)
      .collect()
      .tap((collected) => {
        expect(spy).to.have.been.calledWith(err);
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });
});
