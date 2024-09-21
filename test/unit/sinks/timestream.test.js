import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { writeRecords } from '../../../src/sinks/timestream';

import Connector from '../../../src/connectors/timestream';

describe('sinks/timestream.js', () => {
  afterEach(sinon.restore);

  it('should writeRecords', (done) => {
    const stub = sinon.stub(Connector.prototype, 'writeRecords').resolves({});

    const uows = [{
      writeRequest: {
        DatabaseName: 'd1',
        TableName: 't1',
      },
    }];

    _(uows)
      .through(writeRecords())
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(stub).to.have.been.calledWith({
          DatabaseName: 'd1',
          TableName: 't1',
        });

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          writeRequest: {
            DatabaseName: 'd1',
            TableName: 't1',
          },
          writeResponse: {},
        });
      })
      .done(done);
  });
});
