import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import Connector from '../../../src/connectors/eventbridge';
import S3Connector from '../../../src/connectors/s3';

import { fromKinesis, toKinesisRecords } from '../../../src/from/kinesis';
import {
  faults, flushFaults, FAULT_EVENT_TYPE, limitFaultSize,
} from '../../../src/faults';
import { FAULT_COMPRESSION_IGNORE } from '../../../src/utils/faults';

import { defaultOptions } from '../../../src/utils/opt';
import { compress } from '../../../src/utils/compression';

let publishStub;
let claimcheckStub;

describe('faults/index.js', () => {
  beforeEach(() => {
    process.env.CLAIMCHECK_BUCKET_NAME = 'test-bucket-name';

    publishStub = sinon.stub(Connector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
    claimcheckStub = sinon.stub(S3Connector.prototype, 'putObject').resolves({});
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

        expect(collected[2].event.uow).to.deep.equal({
          pipeline: undefined,
          record: {
            eventSource: 'aws:kinesis',
            eventID: 'shardId-000000000000:1',
            awsRegion: 'us-west-2',
            kinesis: {
              approximateArrivalTimestamp: undefined,
              sequenceNumber: '1',
              data: 'eyJ0eXBlIjoiZjIiLCJlbnRpdHkiOnsiZjEiOiJ2MSIsImYyIjoidjIifSwiZWVtIjp7ImZpZWxkcyI6WyJmMiJdfX0=',
            },
          },
          event: {
            id: 'shardId-000000000000:1',
            type: 'f2',
            entity: {
              f1: 'v1',
              f2: '[REDACTED]',
            },
            eem: { fields: ['f2'] },
          },
          someResponse: {
            f3: '[BUFFER: 2]',
            f4: '[CIRCULAR]',
          },
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

  it('should check fault size', () => {
    const events = [
      {
        type: 'f1',
        body: '123456789012345678901234567890',
      },
      {
        type: 'f2',
        body: '123456789012345678901234567890',
      },
    ];
    const records = toKinesisRecords(events);

    const opt = {
      ...defaultOptions,
      compressionThreshold: 40,
      maxRequestSize: 300,
      compressionIgnore: FAULT_COMPRESSION_IGNORE,
    };

    expect(JSON.parse(JSON.stringify(limitFaultSize({
      type: 'fault',
      uow: {
        record: records.Records[0],
        event: events[0],
      },
    }, opt), compress(opt)))).to.deep.equal({
      type: 'fault',
      uow: {
        record: { // records.Records[0]
          awsRegion: 'us-west-2',
          eventID: 'shardId-000000000000:0',
          eventSource: 'aws:kinesis',
          kinesis: {
            // NOT compressed
            data: 'eyJ0eXBlIjoiZjEiLCJib2R5IjoiMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwIn0=',
            sequenceNumber: '0',
          },
        },
        // event removed
      },
    });

    expect(JSON.parse(JSON.stringify(limitFaultSize({
      type: 'fault',
      uow: {
        batch: [
          {
            record: records.Records[0],
            event: events[0],
          },
          {
            record: records.Records[1],
            event: events[1],
          },
        ],
      },
    }, opt), compress(opt)))).to.deep.equal({
      type: 'fault',
      uow: {
        batch: [
          {
            record: { // records.Records[0]
              awsRegion: 'us-west-2',
              eventID: 'shardId-000000000000:0',
              eventSource: 'aws:kinesis',
              kinesis: {
                // NOT compressed
                data: 'eyJ0eXBlIjoiZjEiLCJib2R5IjoiMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwIn0=',
                sequenceNumber: '0',
              },
            },
            // event removed
          },
          {
            record: { // records.Records[1]
              awsRegion: 'us-west-2',
              eventID: 'shardId-000000000000:1',
              eventSource: 'aws:kinesis',
              kinesis: {
                // NOT compressed
                data: 'eyJ0eXBlIjoiZjIiLCJib2R5IjoiMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwIn0=',
                sequenceNumber: '1',
              },
            },
            // event removed
          },
        ],
      },
    });
  });

  it('should claimcheck large faults', (done) => {
    let e;
    const simulateHandledError = (uow) => {
      if (uow.record.kinesis.sequenceNumber === '1') {
        e = new Error('handled error, has uow attached');
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
          longValue: Array(200).fill('a').join(''),
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
      // 400 used here to be small enough to require a claimcheck but not so small
      // that the claimcheck itself exceeds the maximum publish size.
      .through(flushFaults({ ...defaultOptions, maxPublishRequestSize: 400 }))

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

        expect(collected[2].event.uow).to.deep.equal({
          pipeline: undefined,
          record: {
            eventSource: 'aws:kinesis',
            eventID: 'shardId-000000000000:1',
            awsRegion: 'us-west-2',
            kinesis: {
              approximateArrivalTimestamp: undefined,
              sequenceNumber: '1',
              data: 'eyJ0eXBlIjoiZjIiLCJlbnRpdHkiOnsiZjEiOiJ2MSIsImYyIjoidjIiLCJsb25nVmFsdWUiOiJhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYSJ9LCJlZW0iOnsiZmllbGRzIjpbImYyIl19fQ==',
            },
          },
          event: {
            id: 'shardId-000000000000:1',
            type: 'f2',
            entity: {
              f1: 'v1',
              f2: '[REDACTED]',
              longValue: Array(200).fill('a').join(''),
            },
            eem: { fields: ['f2'] },
          },
          someResponse: {
            f3: '[BUFFER: 2]',
            f4: '[CIRCULAR]',
          },
        });

        // Claimcheck related
        expect(claimcheckStub.calledOnce);
        expect(collected[2].publishRequestEntry).to.deep.equal({
          EventBusName: 'undefined',
          Source: 'custom',
          DetailType: 'fault',
          Detail: `{\"id\":\"${collected[2].event.id}\",\"type\":\"fault\",\"partitionKey\":\"${collected[2].event.partitionKey}\",\"timestamp\":${collected[2].event.timestamp},\"s3\":{\"bucket\":\"test-bucket-name\",\"key\":\"CLAIMCHECK-${collected[2].event.id}\"}}`,
        });
        expect(collected[2].claimcheckRequred);
        expect(collected[2].putClaimcheckRequest).to.deep.equal({
          Key: `CLAIMCHECK-${collected[2].event.id}`,
          Body: JSON.stringify(collected[2].event),
        });
        expect(collected[2].putClaimcheckResponse).to.deep.equal({});
        expect(collected[2].claimcheckEvent).to.deep.equal({
          id: collected[2].event.id,
          partitionKey: collected[2].event.partitionKey,
          timestamp: collected[2].event.timestamp,
          type: collected[2].event.type,
          s3: {
            bucket: 'test-bucket-name',
            key: `CLAIMCHECK-${collected[2].event.id}`,
          },
        });
        expect(collected[2].publishRequest).to.deep.equal({
          Entries: [
            {
              EventBusName: 'undefined',
              Source: 'custom',
              DetailType: 'fault',
              Detail: `{\"id\":\"${collected[2].event.id}\",\"type\":\"fault\",\"partitionKey\":\"${collected[2].event.partitionKey}\",\"timestamp\":${collected[2].event.timestamp},\"s3\":{\"bucket\":\"test-bucket-name\",\"key\":\"CLAIMCHECK-${collected[2].event.id}\"}}`,
            },
          ],
        });
        expect(collected[2].publishResponse).to.deep.equal({
          FailedEntryCount: 0,
        });
      })
      .done(done);
  });
});
