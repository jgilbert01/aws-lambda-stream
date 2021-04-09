import 'mocha';
import { expect } from 'chai';
import debug from 'debug';
import fetchMock from 'fetch-mock';

import Connector from '../../../src/connectors/fetch';

fetchMock.config.Promise = require('bluebird');

describe('connectors/fetch.js', () => {
  afterEach(() => fetchMock.restore());

  const url = 'https://raw.githubusercontent.com/jgilbert01/aws-lambda-stream/master/.babelrc';

  it('should fetch', async () => {
    fetchMock.mock(url, RESPONSE);

    const connector = new Connector({ debug: debug('fetch'), timeout: 3000 });

    const data = await connector.fetch(url, { method: 'GET' });

    // console.log(JSON.stringify(data, null, 2));

    expect(data).to.deep.equal({
      ...RESPONSE,
    });
  });
});

const RESPONSE = {
  presets: [
    [
      '@babel/env',
      {
        targets: {
          node: '12',
        },
      },
    ],
  ],
  plugins: [
    '@babel/plugin-transform-runtime',
  ],
  env: {
    test: {
      plugins: [
        'istanbul',
      ],
    },
  },
};
