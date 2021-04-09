import 'isomorphic-fetch';

import realFetch from 'node-fetch';
import Promise from 'bluebird';

realFetch.Promise = Promise;

class Connector {
  constructor(
    {
      debug,
      httpsAgent,
      timeout = Number(process.env.FETCH_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    },
  ) {
    this.debug = debug;
    this.httpsAgent = httpsAgent;
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
      .then((res) => res[responseType]())
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
