import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { debug } from '../../../src/utils';
import { sendToSqs } from '../../../src/utils/sqs';

import Connector from '../../../src/connectors/sqs';

describe('utils/sqs.js', () => {
  afterEach(sinon.restore);

  it('should batch and publish', (done) => {
    sinon.stub(Connector.prototype, 'sendMessageBatch').resolves({});

    const uows = [{
      message: {
        Id: '1',
        MessageBody: JSON.stringify({ f1: 'v1' }),
      },
    }];

    _(uows)
      .through(sendToSqs({ debug: debug('sqs') }))
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
