import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { toResolve, faulty, faultyAsync } from '../../../src/utils/faults';

describe('utils/faults.js', () => {
  afterEach(sinon.restore);

  it('should handle faulty', (done) => {
    const uows = [{
    }];

    const toUpdateRequest = (rule) => faultyAsync((uow) =>
      toResolve(rule.toUpdateRequest, uow, rule)
        .then((updateRequest) => ({
          ...uow,
          updateRequest,
        })));

    _(uows)
      .flatMap(toUpdateRequest({
        toUpdateRequest: faulty(() => { throw new Error('faulty'); }),
      }))
      // .tap((uow) => console.log(JSON.stringify(uow, null, 2)))
      .errors((err, push) => {
        // console.log(JSON.stringify(err, null, 2));
        if (!err.uow) {
          push(err);
        }
      })
      .stopOnError(done)
      .done(done);
  });
});
