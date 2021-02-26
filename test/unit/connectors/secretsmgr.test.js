import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import AWS from 'aws-sdk-mock';

import Connector from '../../../src/connectors/secretsmgr';

describe('connectors/secretsmgr.js', () => {
  afterEach(() => {
    AWS.restore('SecretsManager');
  });

  it('should get the secret', async () => {
    const SecretString = Buffer.from(JSON.stringify({ MY_SECRET: '123456' })).toString('base64');
    // use this string in the fixtures/.../secrets recording
    console.log('SecretString: ', SecretString);

    const spy = sinon.spy((params, cb) => cb(null, {
      ARN: 'arn:aws:secretsmanager:us-west-2:123456789012:secret:MyTestDatabaseSecret-xxxxxx',
      CreatedDate: '<Date Representation>',
      Name: 'my-service/tst',
      SecretString,
      VersionId: 'EXAMPLE1-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
      VersionStages: [
        'AWSPREVIOUS',
      ],
    }));

    AWS.mock('SecretsManager', 'getSecretValue', spy);

    const connector = new Connector({ debug: debug('sm'), secretId: 'my-service/tst' });

    let secrets = await connector.get();
    expect(secrets).to.deep.equal({ MY_SECRET: '123456' });

    secrets = await connector.get();
    expect(secrets).to.deep.equal({ MY_SECRET: '123456' });

    // assert cached
    expect(spy).to.have.been.calledOnceWith({
      SecretId: 'my-service/tst',
    });
  });
});
