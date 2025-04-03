import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  adornStandardTags,
} from '../../../src/utils';

describe('utils/tags.js', () => {
  afterEach(sinon.restore);

  it('should adorn tags, event field exists', () => {
    process.env.ACCOUNT_NAME = 'account1';
    process.env.STAGE = 'dev';
    process.env.SERVICE = 'service1';
    process.env.AWS_LAMBDA_FUNCTION_NAME = 'lambda1';
    const uow = {
      pipeline: 'thing-pipeline',
      emit: {
        type: 'thing-updated',
        partitionKey: 'thing1',
        timestamp: 1600144863435,
        thing: {
          id: '7588777d-19a0-49d5-856f-a657ed4b5034',
          timestamp: 1600144863435,
        },
      },
    };
    const event = adornStandardTags('emit')(uow);
    delete process.env.ACCOUNT_NAME;
    delete process.env.STAGE;
    delete process.env.SERVICE;
    delete process.env.AWS_LAMBDA_FUNCTION_NAME;
    expect(event).to.deep.equal({
      ...uow,
      emit: {
        ...uow.emit,
        tags: {
          account: 'account1',
          region: 'us-west-2',
          stage: 'dev',
          source: 'service1',
          functionname: 'lambda1',
          pipeline: 'thing-pipeline',
          skip: true,
        },
      },
    });
  });

  it('should not adorn tags, event field does not exist', () => {
    const uow = {
      pipeline: 'thing-pipeline',
    };
    const event = adornStandardTags('emit')(uow);
    expect(event).to.deep.equal(uow);
  });

  it('should not adorn tags, event field is empty object', () => {
    const uow = {
      pipeline: 'thing-pipeline',
      emit: {},
    };
    const event = adornStandardTags('emit')(uow);
    expect(event).to.deep.equal(uow);
  });
});
