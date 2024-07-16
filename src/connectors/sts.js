/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { STSClient, AssumeRoleCommand } from '@aws-sdk/client-sts';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    timeout = Number(process.env.STS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.client = Connector.getClient(debug, timeout);
  }

  static _client;

  static getClient(debug, timeout) {
    if (!this._client) {
      this._client = new STSClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
        }),
        logger: defaultDebugLogger(debug),
      });
    }
    return this._client;
  }

  assumeRole(params) {
    const command = new AssumeRoleCommand(params);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;

// middleware
export const assumeRole = async (next, opt) => {
  const connector = new Connector(opt);

  const data = await connector.assumeRole({
    RoleArn: process.env.ASSUME_ROLE,
    RoleSessionName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'aws-lambda-stream',
  });

  opt.credentials = {
    accessKeyId: data.Credentials.AccessKeyId,
    secretAccessKey: data.Credentials.SecretAccessKey,
    sessionToken: data.Credentials.SessionToken,
  };

  return next();
};
