import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import dogapi from 'dogapi';

import Connector from '../../../src/connectors/datadog';

const debug = require('debug')('dogapi');

describe('connector/datadog.js', () => {
  afterEach(sinon.restore);

  it('should initialize', async () => {
    const stub = sinon.stub(dogapi, 'initialize').returns({});

    const c = new Connector({
      debug,
      apiKey: 'apiKey',
      appKey: 'appKey',
    });

    expect(c).to.not.undefined;
    expect(stub).to.have.been.calledWith({
      api_key: 'apiKey',
      app_key: 'appKey',
      api_host: 'ddog-gov.com',
      // proxy_agent: undefined,
    });
  });

  it('should send an event', async () => {
    const stub = sinon.stub(dogapi.event, 'create')
      .callsFake((title, message, props, callback) => {
        callback(null, true);
      });

    const data = await new Connector({
      debug,
      apiKey: 'apiKey',
      appKey: 'appKey',
    }).sendEvent('title', 'msg');

    expect(stub).to.have.been.calledWith('title', 'msg');
    expect(data).to.deep.equal(true);
  });

  it('should not send an event', async () => {
    const stub = sinon.stub(dogapi.event, 'create')
      .callsFake((title, message, props, callback) => {
        callback('error');
      });

    const data = await new Connector({
      debug,
      apiKey: 'apiKey',
      appKey: 'appKey',
    }).sendEvent('title', 'msg');

    expect(stub).to.have.been.calledWith('title', 'msg');
    expect(data).to.deep.equal('error');
  });
});
