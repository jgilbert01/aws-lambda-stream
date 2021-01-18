import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { toKinesisRecords } from 'aws-lambda-stream';

import Connector from '../../../src/connectors/cloudwatch';
import { handle, Handler } from '../../../src/listener';

describe('listener/index.js', () => {
  afterEach(sinon.restore);

  it('should handle events', (done) => {
    const stub = sinon.stub(Connector.prototype, 'put')
      .resolves({});

    new Handler().handle(EVENTS)
      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(4);
        // TODO Namespace
        expect(collected[0].MetricData[0]).to.deep.equal({
          MetricName: 'domain.event',
          Timestamp: 1441121400,
          Unit: 'Count',
          Value: 1,
          Dimensions: [
            {
              Name: 'account',
              Value: 'not-specified',
            },
            {
              Name: 'region',
              Value: 'us-west-2',
            },
            {
              Name: 'shard',
              Value: '000000000000',
            },
            {
              Name: 'stage',
              Value: 'not-specified',
            },
            {
              Name: 'source',
              Value: 'not-specified',
            },
            {
              Name: 'functionname',
              Value: 'not-specified',
            },
            {
              Name: 'pipeline',
              Value: 'not-specified',
            },
            {
              Name: 'type',
              Value: 'blue',
            },
          ],
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
