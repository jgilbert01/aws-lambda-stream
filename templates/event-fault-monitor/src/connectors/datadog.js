import dogapi from 'dogapi';
import Promise from 'bluebird';
import HttpsProxyAgent from 'https-proxy-agent';

class Connector {
  constructor({
    debug,
    apiKey,
    appKey,
  }) {
    this.debug = debug;

    if (!dogapi.client.api_key) {
      dogapi.initialize({
        api_key: apiKey,
        app_key: appKey,
        api_host: 'ddog-gov.com',
        // proxy_agent: proxy(),
      });
    }
  }

  sendEvent(title, message, properties) {
    return new Promise((resolve) => {
      dogapi.event.create(title, message, properties, (err, res) => {
        if (err) {
          console.log('DOGAPI ERROR: %j', {
            err,
            title,
            message,
            properties,
          });
          resolve(err);
        } else {
          resolve(res);
        }
      });
    })
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;

// export const proxy = () => (process.env.NODE_ENV === 'test'
//   ? undefined
//   : /* istanbul ignore next */ new HttpsProxyAgent({
//     protocol: 'https',
//     host: 'proxy.ice.dhs.gov',
//   }));
