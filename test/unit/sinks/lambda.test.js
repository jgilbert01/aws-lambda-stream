import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  invokeLambda,
  createEventSourceMapping,
  updateEventSourceMapping,
  deleteEventSourceMapping,
} from '../../../src/sinks/lambda';

import Connector from '../../../src/connectors/lambda';

describe('sinks/lambda.js', () => {
  afterEach(sinon.restore);

  it('should invoke lambda', (done) => {
    const stub = sinon.stub(Connector.prototype, 'invoke').resolves({});

    const uows = [{
      invokeRequest: {
        FunctionName: 'helloworld',
      },
    }];

    _(uows)
      .through(invokeLambda())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          FunctionName: 'helloworld',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          invokeRequest: {
            FunctionName: 'helloworld',
          },
          invokeResponse: {},
        });
      })
      .done(done);
  });

  it('should create esm', (done) => {
    const stub = sinon.stub(Connector.prototype, 'createEventSourceMapping').resolves({});

    const uows = [{
      createRequest: {
        Enabled: false,
        BatchSize: 10,
      },
    }, {
      createRequest: undefined,
    }];

    _(uows)
      .through(createEventSourceMapping())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          Enabled: false,
          BatchSize: 10,
        });

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal({
          createRequest: {
            Enabled: false,
            BatchSize: 10,
          },
          createResponse: {},
        });
      })
      .done(done);
  });

  it('should update esm', (done) => {
    const stub = sinon.stub(Connector.prototype, 'updateEventSourceMapping').resolves({});

    const uows = [{
      updateRequest: {
        UUID: '1',
        Enabled: false,
        BatchSize: 10,
      },
    }, {
      updateRequest: undefined,
    }];

    _(uows)
      .through(updateEventSourceMapping())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          UUID: '1',
          Enabled: false,
          BatchSize: 10,
        });

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal({
          updateRequest: {
            UUID: '1',
            Enabled: false,
            BatchSize: 10,
          },
          updateResponse: {},
        });
      })
      .done(done);
  });

  it('should delete esm', (done) => {
    const stub = sinon.stub(Connector.prototype, 'deleteEventSourceMapping').resolves({});

    const uows = [{
      deleteRequest: {
        UUID: '1',
      },
    }, {
      deleteRequest: undefined,
    }];

    _(uows)
      .through(deleteEventSourceMapping())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(stub).to.have.been.calledWith({
          UUID: '1',
        });

        expect(collected.length).to.equal(2);
        expect(collected[0]).to.deep.equal({
          deleteRequest: {
            UUID: '1',
          },
          deleteResponse: {},
        });
      })
      .done(done);
  });
});
