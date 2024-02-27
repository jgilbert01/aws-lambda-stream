import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { debug } from '../../../src/utils';
import { sendToFirehose, toFirehoseRecord } from '../../../src/sinks/firehose';

import Connector from '../../../src/connectors/firehose';

describe('utils/firehose.js', () => {
  afterEach(sinon.restore);

  it('should batch and put', (done) => {
    sinon.stub(Connector.prototype, 'putRecordBatch').resolves({});

    const uows = [{
      event: {
        id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
        type: 'p1',
        partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
      },
    }];

    _(uows)
      .through(sendToFirehose({ debug: debug('firehose') }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          event: {
            id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
            type: 'p1',
            partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
          },
          inputParams: {
            Records: [
              toFirehoseRecord({
                id: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
                type: 'p1',
                partitionKey: '79a0d8f0-0eef-11ea-8d71-362b9e155667',
              }),
            ],
          },
          putResponse: {},
        });
      })
      .done(done);
  });

  it('should reject with a fault', (done) => {
    sinon.stub(Connector.prototype, 'putRecordBatch').rejects('test error');

    const uows = [{
      event: {
        id: '14f46ef2-0ef0-11ea-8d71-362b9e155667',
        type: 'p2',
        partitionKey: 'f440c880-4c41-4965-8658-2cbd503a2c73',
      },
    }];

    _(uows)
      .through(sendToFirehose())
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
              },
            },
          ],
          inputParams: {
            Records: [
              toFirehoseRecord({
                id: '14f46ef2-0ef0-11ea-8d71-362b9e155667',
                type: 'p2',
                partitionKey: 'f440c880-4c41-4965-8658-2cbd503a2c73',
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
