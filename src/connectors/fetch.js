import 'isomorphic-fetch';

import realFetch, { FetchError } from 'node-fetch';
import Promise from 'bluebird';

realFetch.Promise = Promise;

class Connector {
  constructor(
    {
      debug,
      httpsAgent,
      defaultResponse = { 204: {} },
      timeout = Number(process.env.FETCH_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    },
  ) {
    this.debug = debug;
    this.log = (msg) => this.debug('%j', msg);
    this.httpsAgent = httpsAgent;
    this.defaultResponse = defaultResponse;
    this.timeout = timeout;
  }

  fetch(url, request, responseType = 'json') {
    this.debug('%j', {
      url,
      responseType,
      agent: this.httpsAgent,
      timeout: this.timeout,
      ...request,
    });

    return fetch(url, {
      agent: this.httpsAgent,
      timeout: this.timeout,
      ...request,
    })
      .tap(this.log)
      .then((res) => (res.ok ? res : Promise.reject(new FetchError(`HTTP Error Response: ${res.status} ${res.statusText}`, 'status-code'))))
      .then((res) => (res.status === 204 ? this.defaultResponse[204][responseType] : res[responseType]()))
      .tap(this.log)
      .tapCatch(this.log);
  }
}

export default Connector;
