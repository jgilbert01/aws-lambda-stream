import 'mocha';
import { expect } from 'chai';
import debug from 'debug';
import fetchMock from 'fetch-mock';

import Connector from '../../../src/connectors/fetch';

describe.only('connectors/fetch.js', () => {
  afterEach(() => fetchMock.restore());

  const url = 'http://www.google.com/search?q=node-fetch';

  it('should get and auth user', async () => {
    // fetchMock.mock(
    //   url,
    //   RESPONSE,
    // );

    const connector = new Connector({ debug: debug('fetch'), timeout: 3000 });

    const data = await connector.fetch(url, { method: 'GET' }, 'text');

    console.log(JSON.stringify(data, null, 2));

    expect(data).to.deep.equal({
    });
  });
});

const RESPONSE = {
};
