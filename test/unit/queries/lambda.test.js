import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { listEventSourceMappings } from '../../../src/queries/lambda';

import Connector from '../../../src/connectors/lambda';

describe('queries/lambda.js', () => {
  afterEach(sinon.restore);

  it('should list ESMs', (done) => {
    const stub = sinon.stub(Connector.prototype, 'listEventSourceMappings').resolves({
      EventSourceMappings: [
        {
          UUID: '1',
          FunctionArn: 'arn:f1',
        },
      ],
      NextMarker: undefined,
    });

    const uows = [{
      listRequest: {
        FunctionName: 'f1',
      },
    }];

    _(uows)
      .through(listEventSourceMappings())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          FunctionName: 'f1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          listRequest: {
            FunctionName: 'f1',
          },
          listResponse: {
            EventSourceMappings: [
              {
                UUID: '1',
                FunctionArn: 'arn:f1',
              },
            ],
            NextMarker: undefined,
          },
        });
      })
      .done(done);
  });
});
