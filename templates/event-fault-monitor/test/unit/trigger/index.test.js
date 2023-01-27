import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { handle, Handler } from '../../../src/trigger';

describe('trigger/index.js', () => {
  afterEach(sinon.restore);

  it('should verify Handler', (done) => {
    new Handler()
      .handle({
        Records: ([{
          body: JSON.stringify({
            Message: JSON.stringify({
              Records: [
                // {
                //   s3: {
                //     bucket: {
                //       name: 'eagle-ng-event-fault-monitor-stg-bucket-irt6nbuebsp2',
                //     },
                //     object: {
                //       key: 'us-gov-west-1/2021/03/29/16/eagle-ng-event-fault-monitor-stg-DeliveryStream-1D9aFKD14ZHZ-1-2021-03-29-16-18-54-314e0436-fd67-422a-ac66-cd2705d0ac11',
                //     },
                //   },
                // }
              ],
            }),
          }),
        }]),
      })
      .collect()
      .tap((collected) => {
        expect(collected.length).to.equal(0);
      })
      .done(done);
  });

  it('should test successful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.of({}));

    const res = await handle({}, {});

    expect(spy).to.have.been.calledWith({});
    expect(res).to.equal('Success');
  });

  it('should test unsuccessful handle call', async () => {
    const spy = sinon.stub(Handler.prototype, 'handle').returns(_.fromError(Error()));

    try {
      await handle({}, {});
      expect.fail('expected error');
    } catch (e) {
      expect(spy).to.have.been.calledWith({});
    }
  });
});
