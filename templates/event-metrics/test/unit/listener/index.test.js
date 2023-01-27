import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { toKinesisRecords, CloudWatchConnector as Connector } from 'aws-lambda-stream';

import { handle, Handler } from '../../../src/listener';

describe('listener/index.js', () => {
  afterEach(sinon.restore);

  it('should handle events', (done) => {
    const stub = sinon.stub(Connector.prototype, 'put')
      .resolves({});

    new Handler().handle(EVENTS)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(4);
        expect(collected[0].embeddedMetrics).to.deep.equal({
          '_aws': {
            Timestamp: 1441121400000,
            CloudWatchMetrics: [
              {
                Namespace: undefined,
                Dimensions: [
                  [
                    'account',
                    'region',
                    'stream',
                    'shard',
                    'stage',
                    'source',
                    'functionname',
                    'pipeline',
                    'type',
                  ],
                ],
                Metrics: [
                  {
                    Name: 'domain.event',
                    Unit: 'Count',
                  },
                  {
                    Name: 'domain.event.size',
                    Unit: 'Bytes',
                  },
                ],
              },
            ],
          },
          'account': 'not-specified',
          'region': 'us-west-2',
          'stream': 'not-specified',
          'shard': '000000000000',
          'stage': 'not-specified',
          'source': 'not-specified',
          'functionname': 'not-specified',
          'pipeline': 'not-specified',
          'type': 'blue',
          'domain.event': 1,
          'domain.event.size': 56,
        });
      })
      .done(done);
  });

  it('should test successful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.of({}));

    const res = await handle({}, {});

    expect(spy).to.have.been.calledWith({});
    expect(res).to.equal('Success');
  });

  it('should test unsuccessful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.fromError(Error()));

    try {
      await handle({}, {});
      expect.fail('expected error');
    } catch (e) {
      expect(spy).to.have.been.calledWith({});
    }
  });
});

const EVENTS = toKinesisRecords([
  {
    type: 'blue',
    timestamp: 1441121400000,
  },
  {
    type: 'red',
    timestamp: 1441121500000,
  },
  {
    type: 'purple',
    timestamp: 1441121600000,
    tags: {
      account: 'dev',
      region: 'us-east-1',
      stg: 'stg',
      source: 's1',
      functionname: 'f1',
    },
  },
  {
    type: 'fault',
    timestamp: 1441121600000,
    tags: {
      account: 'dev',
      functionname: 'f1',
      pipeline: 'p1',
    },
    err: {
      name: 'Error',
      message: 'this is an error',
      stack: 'the stack trace',
    },
  },
]);

// .Records.map(r => ({
//   ...r,
//   eventSourceARN: 'arn:aws:kinesis:us-east-1:123456789012:stream/test_stream',
// })),
