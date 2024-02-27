import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { getSecrets } from '../../../src/queries/secretsmgr';

import Connector from '../../../src/connectors/secretsmgr';

describe('utils/secretsmgr.js', () => {
  afterEach(sinon.restore);

  it('should get secrets', async () => {
    sinon.stub(Connector.prototype, 'get').resolves({ MY_SECRET: '123456' });

    const options = await getSecrets({ opt1: 1 });

    expect(options).to.deep.equal({ opt1: 1, MY_SECRET: '123456' });
  });
});
