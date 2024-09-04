import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import debug from 'debug';
import { STSClient, AssumeRoleCommand } from '@aws-sdk/client-sts';
import { mockClient } from 'aws-sdk-client-mock';

import Connector, { assumeRole } from '../../../src/connectors/sts';
import { mw } from '../../../src/utils';

describe('connectors/sts.js', () => {
  let mockSTS;

  beforeEach(() => {
    mockSTS = mockClient(STSClient);
  });

  afterEach(() => {
    mockSTS.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));

    expect(client1).to.eq(client2);
  });

  it('should assume role', async () => {
    const spy = sinon.spy((_) => ({
      Credentials: {
        AccessKeyId: 'aki',
        SecretAccessKey: 'sak',
        SessionToken: 'st',
        CredentialScope: 'cs',
      },
    }));
    mockSTS.on(AssumeRoleCommand).callsFake(spy);

    const data = await new Connector({ debug: debug('cw') })
      .assumeRole({ RoleArn: 'ns', RoleSessionName: 'test' });

    expect(spy).to.have.been.calledOnce;
    expect(spy).to.have.been.calledWith({
      RoleArn: 'ns', RoleSessionName: 'test',
    });
    expect(data).to.deep.equal({
      Credentials: {
        AccessKeyId: 'aki',
        SecretAccessKey: 'sak',
        SessionToken: 'st',
        CredentialScope: 'cs',
      },
    });
  });

  it('should execute middleware', async () => {
    const stub = sinon.stub(Connector.prototype, 'assumeRole').resolves({
      Credentials: {
        AccessKeyId: 'aki',
        SecretAccessKey: 'sak',
        SessionToken: 'st',
        CredentialScope: 'cs',
      },
    });

    const handle = (event, context, options) => Promise.resolve(options);

    const result = await mw(handle, { opt1: 1 })
      .use(assumeRole)({}, {});

    expect(stub).to.have.been.called;
    expect(result).to.deep.equal({
      opt1: 1,
      credentials: {
        accessKeyId: 'aki',
        secretAccessKey: 'sak',
        sessionToken: 'st',
      },
    });
  });
});
