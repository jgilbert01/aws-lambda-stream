import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { sendToSqs } from '../../../src/sinks/sqs';

import Connector from '../../../src/connectors/sqs';

describe('utils/sqs.js', () => {
  afterEach(sinon.restore);

  it('should batch and send', (done) => {
    sinon.stub(Connector.prototype, 'sendMessageBatch').resolves({});

    const uows = [{
      message: {
        Id: '1',
        MessageBody: JSON.stringify({ f1: 'v1' }),
      },
    }];

    _(uows)
      .through(sendToSqs())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          message: {
            Id: '1',
            MessageBody: JSON.stringify({ f1: 'v1' }),
          },
          inputParams: {
            Entries: [{
              Id: '1',
              MessageBody: JSON.stringify({ f1: 'v1' }),
            }],
          },
          sendMessageBatchResponse: {},
        });
      })
      .done(done);
  });
});
