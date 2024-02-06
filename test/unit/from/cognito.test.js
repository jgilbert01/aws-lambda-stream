import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { uuid } from '../../../src/utils';

import { fromCognito } from '../../../src/from/cognito';

const postConfirmationEvent = {
  version: '1',
  triggerSource: 'PostConfirmation_ConfirmSignUp',
  region: process.env.AWS_REGION,
  userPoolId: 'us-west-2_WvvYlc2XP',
  userName: '5e44e08e-4b70-4eac-b710-2b86fbeddad8',
  callerContext: {
    awsSdkVersion: '1.2.123',
    clientId: '3gd7kk5ml13s821fi8uimm73mu',
  },
  request: {
    userAttributes: {
      sub: '091c283f-61b0-47aa-916d-bec63211d363',
      given_name: 'FAKEY',
      family_name: 'MCFAKERSON',
      email: 'fakey.mcfakerson@email.com',
    },
  },
  response: {},
};

describe('from/cognito.js', () => {
  beforeEach(() => {
    sinon.stub(uuid, 'v4').returns('221c5dbf-e314-4c75-97b5-f47c46f6b171');
  });

  afterEach(sinon.restore);
  it('should parse records', (done) => {
    fromCognito(postConfirmationEvent)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: postConfirmationEvent,
          event: {
            id: '221c5dbf-e314-4c75-97b5-f47c46f6b171',
            type: 'aws-cognito-post-confirmation-confirm-sign-up',
            partitionKey: '5e44e08e-4b70-4eac-b710-2b86fbeddad8',
            tags: {
              region: 'us-west-2',
              source: 'us-west-2_WvvYlc2XP',
            },
            raw: {
              userAttributes: {
                email: 'fakey.mcfakerson@email.com',
                family_name: 'MCFAKERSON',
                given_name: 'FAKEY',
                sub: '091c283f-61b0-47aa-916d-bec63211d363',
              },
            },
          },
        });
      })
      .done(done);
  });
});
