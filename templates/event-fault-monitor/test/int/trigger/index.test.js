import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import moment from 'moment';

import { handle } from '../../../src/trigger';

describe('trigger/index.js', () => {
  before(() => {
    require('baton-vcr-replay-for-aws-sdk'); // eslint-disable-line global-require

    sinon.stub(moment.prototype, 'unix').returns(1645651231);
  });

  it('should test trigger integration', async () => {
    const res = await handle(EVENT, {});
    expect(res).to.equal('Success');
  });
});

const EVENT = {
  Records: ([{
    body: JSON.stringify({
      Message: JSON.stringify({
        Records: [{
          s3: {
            bucket: {
              name: 'eagle-ng-event-fault-monitor-stg-bucket-irt6nbuebsp2',
            },
            object: {
              key: 'us-gov-west-1/2021/03/29/16/eagle-ng-event-fault-monitor-stg-DeliveryStream-1D9aFKD14ZHZ-1-2021-03-29-16-18-54-314e0436-fd67-422a-ac66-cd2705d0ac11',
            },
          },
        }],
      }),
    }),
  }]),
};
