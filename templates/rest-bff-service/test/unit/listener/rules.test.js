import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toKinesisRecords, DynamoDBConnector } from 'aws-lambda-stream';

import { Handler } from '../../../src/listener';

describe('listener/rules.js', () => {
  beforeEach(() => {
    sinon.stub(DynamoDBConnector.prototype, 'update').resolves({});
  });
  afterEach(sinon.restore);

  it('should verify collect rule clt1', (done) => {
    new Handler()
      .handle(toKinesisRecords([
        {
          type: 'thing-submitted',
          thing: {
            id: '00000000-0000-0000-0000-000000000000',
            name: 'thing0',
          },
        },
      ]), false)
      .collect()
      // .tap(collected => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(1);
        expect(collected[0].pipeline).to.equal('l1');
        expect(collected[0].event.type).to.equal('thing-submitted');
      })
      .done(done);
  });
});
