import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { sendToSqs } from '../../../src/sinks/sqs';

import Connector from '../../../src/connectors/sqs';

describe('sinks/sqs.js', () => {
  afterEach(sinon.restore);

  it('should batch and send', (done) => {
    sinon.stub(Connector.prototype, 'sendMessageBatch').resolves({});

    const uows = [{
      message: {
        Id: '1',
        MessageBody: JSON.stringify({ f1: 'v1' }),
      },
    }, {
      message: {
        Id: '2',
        MessageBody: JSON.stringify({ f1: 'v2' }),
      },
    }];

    _(uows)
      .through(sendToSqs())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal({
          message: {
            Id: '1',
            MessageBody: JSON.stringify({ f1: 'v1' }),
          },
          inputParams: {
            Entries: [{
              Id: '1',
              MessageBody: JSON.stringify({ f1: 'v1' }),
            }, {
              Id: '2',
              MessageBody: JSON.stringify({ f1: 'v2' }),
            }],
          },
          sendMessageBatchResponse: {},
        });

        expect(collected[1]).to.deep.equal({
          message: {
            Id: '2',
            MessageBody: JSON.stringify({ f1: 'v2' }),
          },
          inputParams: {
            Entries: [{
              Id: '1',
              MessageBody: JSON.stringify({ f1: 'v1' }),
            }, {
              Id: '2',
              MessageBody: JSON.stringify({ f1: 'v2' }),
            }],
          },
          sendMessageBatchResponse: {},
        });
      })
      .done(done);
  });

  it('should split a batch due to batch size', (done) => {
    sinon.stub(Connector.prototype, 'sendMessageBatch').resolves({});

    const uows = [{
      message: {
        Id: '1',
        MessageBody: JSON.stringify({ f1: 'v1' }),
      },
    }, {
      message: {
        Id: '2',
        MessageBody: JSON.stringify({ f1: 'v2' }),
      },
    }];

    _(uows)
      .through(sendToSqs({
        batchSize: 1,
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
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
        expect(collected[1]).to.deep.equal({
          message: {
            Id: '2',
            MessageBody: JSON.stringify({ f1: 'v2' }),
          },
          inputParams: {
            Entries: [{
              Id: '2',
              MessageBody: JSON.stringify({ f1: 'v2' }),
            }],
          },
          sendMessageBatchResponse: {},
        });
      })
      .done(done);
  });

  it('should split a batch due to payload size', (done) => {
    sinon.stub(Connector.prototype, 'sendMessageBatch').resolves({});

    const uows = [{
      message: {
        Id: '1',
        MessageBody: JSON.stringify({ f1: 'v1' }),
      },
    }, {
      message: {
        Id: '2',
        MessageBody: JSON.stringify({ f1: 'v2' }),
      },
    }];

    _(uows)
      .through(sendToSqs({
        maxPayloadSize: 50,
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(2);
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
        expect(collected[1]).to.deep.equal({
          message: {
            Id: '2',
            MessageBody: JSON.stringify({ f1: 'v2' }),
          },
          inputParams: {
            Entries: [{
              Id: '2',
              MessageBody: JSON.stringify({ f1: 'v2' }),
            }],
          },
          sendMessageBatchResponse: {},
        });
      })
      .done(done);
  });
});
