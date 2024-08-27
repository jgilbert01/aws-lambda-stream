import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';

import { mockClient } from 'aws-sdk-client-mock';
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import Connector from '../../../src/connectors/secretsmgr';

describe('connectors/secretsmgr.js', () => {
  let mockSecretsMgr = mockClient(SecretsManagerClient);

  beforeEach(() => {
    mockSecretsMgr = mockClient(SecretsManagerClient);
  });

  afterEach(() => {
    mockSecretsMgr.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should get the secret', async () => {
    const SecretString = Buffer.from(JSON.stringify({ MY_SECRET: '123456' })).toString('base64');
    // use this string in the fixtures/.../secrets recording
    // console.log('SecretString: ', SecretString);

    const spy = sinon.spy(() => ({
      ARN: 'arn:aws:secretsmanager:us-west-2:123456789012:secret:MyTestDatabaseSecret-xxxxxx',
      CreatedDate: '<Date Representation>',
      Name: 'my-service/tst',
      SecretString,
      VersionId: 'EXAMPLE1-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
      VersionStages: [
        'AWSPREVIOUS',
      ],
    }));

    mockSecretsMgr.on(GetSecretValueCommand).callsFake(spy);

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

  it('should use a decode function override', async () => {
    const SecretString = JSON.stringify({ MY_SECRET: '123456' });

    const spy = sinon.spy(() => ({
      ARN: 'arn:aws:secretsmanager:us-west-2:123456789012:secret:MyTestDatabaseSecret-xxxxxx',
      CreatedDate: '<Date Representation>',
      Name: 'my-service/tst',
      SecretString,
      VersionId: 'EXAMPLE1-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
      VersionStages: [
        'AWSPREVIOUS',
      ],
    }));

    mockSecretsMgr.on(GetSecretValueCommand).callsFake(spy);

    const connector = new Connector({ debug: debug('sm'), secretId: 'my-service/tst', decodeFn: (data) => JSON.parse(data) });

    let secrets = await connector.get();
    expect(secrets).to.deep.equal({ MY_SECRET: '123456' });

    secrets = await connector.get();
    expect(secrets).to.deep.equal({ MY_SECRET: '123456' });

    // assert cached
    expect(spy).to.have.been.calledOnceWith({
      SecretId: 'my-service/tst',
    });
  });

  it('should return a rejected promise if decode function fails', async () => {
    const SecretString = `${JSON.stringify({ MY_SECRET: '123456' })}-this-will-fail`;

    const spy = sinon.spy(() => ({
      ARN: 'arn:aws:secretsmanager:us-west-2:123456789012:secret:MyTestDatabaseSecret-xxxxxx',
      CreatedDate: '<Date Representation>',
      Name: 'my-service/tst',
      SecretString,
      VersionId: 'EXAMPLE1-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
      VersionStages: [
        'AWSPREVIOUS',
      ],
    }));

    mockSecretsMgr.on(GetSecretValueCommand).callsFake(spy);

    const connector = new Connector({ debug: debug('sm'), secretId: 'my-service/tst', decodeFn: (data) => JSON.parse(data) });

    const secrets = await connector.get().catch((e) => {
      expect(e.name).to.equal('SyntaxError');
      return 'Failed.';
    });
    expect(secrets).to.equal('Failed.');

    // assert cached
    expect(spy).to.have.been.calledOnceWith({
      SecretId: 'my-service/tst',
    });
  });
});
