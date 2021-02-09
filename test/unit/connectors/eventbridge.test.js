import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/eventbridge';

import { debug } from '../../../src/utils';

describe('connectors/eventbridge.js', () => {
  afterEach(() => {
    AWS.restore('EventBridge');
  });

  it('should publish', async () => {
    const spy = sinon.spy((params, cb) => cb(null, {}));
    AWS.mock('EventBridge', 'putEvents', spy);

    const inputParams = {
      Entries: [
        {
          EventBusName: 'b1',
          DetailType: 't1',
          Detail: JSON.stringify({ type: 't1' }),
        },
      ],
    };

    const data = await new Connector({
      debug: debug('eventbridge'),
    }).putEvents(inputParams);

    expect(spy).to.have.been.calledWith({
      Entries: inputParams.Entries,
    });
    expect(data).to.deep.equal({});
  });
});
