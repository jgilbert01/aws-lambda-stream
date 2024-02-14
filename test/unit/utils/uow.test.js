import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  trimAndRedact,
} from '../../../src/utils/uow';

describe('utils/uow.js', () => {
  afterEach(sinon.restore);

  it('should redact batch uow', () => {
    const uow = {
      batch: [
        {
          pipeline: 'p1',
          record: {
            f2: 'v2', // not touched
          },
          event: {
            type: 'f2',
            entity: {
              f1: 'v1',
              f2: 'v2',
            },
            eem: { fields: ['f2'] },
          },
          decryptResponse: {},
        },
      ],
      inputParams: {
        f2: 'v2',
      },
    };

    expect(trimAndRedact(uow)).to.deep.equal({
      batch: [
        {
          pipeline: 'p1',
          record: {
            f2: 'v2',
          },
          event: {
            type: 'f2',
            entity: {
              f1: 'v1',
              f2: '[REDACTED]',
            },
            eem: { fields: ['f2'] },
          },
        },
      ],
      inputParams: {
        f2: '[REDACTED]',
      },
    });
  });
});
