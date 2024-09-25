import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { debug } from '../../../src/utils';
import { publishToKinesis as publish, toRecord } from '../../../src/sinks/kinesis';

import Connector from '../../../src/connectors/kinesis';

describe('sinks/kinesis.js', () => {
  afterEach(sinon.restore);

  it('should batch and publish', (done) => {
    sinon.stub(Connector.prototype, 'putRecords').resolves({});

    const uows = [{
      event: {
        id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
        type: 'p1',
        partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
      },
    }];

    _(uows)
      .through(publish({ debug: debug('kinesis') }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          event: {
            id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
            type: 'p1',
            partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
            tags: {
              account: 'undefined',
              functionname: 'undefined',
              pipeline: 'undefined',
              region: 'us-west-2',
              source: 'undefined',
              stage: 'undefined',
              skip: true,
            },
          },
          inputParams: {
            Records: [
              toRecord({
                id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
                type: 'p1',
                partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
                tags: {
                  account: 'undefined',
                  region: 'us-west-2',
                  stage: 'undefined',
                  source: 'undefined',
                  functionname: 'undefined',
                  pipeline: 'undefined',
                  skip: true,
                },
              }),
            ],
          },
          publishResponse: {},
        });
      })
      .done(done);
  });

  it('should not publish', (done) => {
    const uows = [{
    }];

    _(uows)
      .through(publish())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0].publishResponse).to.be.undefined;
      })
      .done(done);
  });

  it('should reject with a fault', (done) => {
    sinon.stub(Connector.prototype, 'putRecords').rejects('test error');

    const uows = [{
      event: {
        id: '14f46ef2-0ef0-11ea-8d71-362b9e155667',
        type: 'p2',
        partitionKey: 'f440c880-4c41-4965-8658-2cbd503a2c73',
      },
    }];

    _(uows)
      .through(publish())
      .errors((err) => {
        // console.log(JSON.stringify(err, null, 2));

        expect(err.name).to.equal('test error');
        expect(err.uow).to.deep.equal({
          batch: [
            {
              event: {
                id: '14f46ef2-0ef0-11ea-8d71-362b9e155667',
                type: 'p2',
                partitionKey: 'f440c880-4c41-4965-8658-2cbd503a2c73',
                tags: {
                  account: 'undefined',
                  functionname: 'undefined',
                  pipeline: 'undefined',
                  region: 'us-west-2',
                  source: 'undefined',
                  stage: 'undefined',
                  skip: true,
                },
              },
            },
          ],
          inputParams: {
            Records: [
              toRecord({
                id: '14f46ef2-0ef0-11ea-8d71-362b9e155667',
                type: 'p2',
                partitionKey: 'f440c880-4c41-4965-8658-2cbd503a2c73',
                tags: {
                  account: 'undefined',
                  region: 'us-west-2',
                  stage: 'undefined',
                  source: 'undefined',
                  functionname: 'undefined',
                  pipeline: 'undefined',
                  skip: true,
                },
              }),
            ],
          },
        });
      })
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });
});
