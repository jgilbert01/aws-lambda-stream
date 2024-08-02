/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { omit, pick } from 'lodash';
import { defaultDebugLogger } from '../utils/log';

/**
 * All secrets are combined to reduce the number of calls
 * Secrets are transferred from a ci/cd pipeline's secured variables
 * to SecretsManager using the serverless-secrets-mgr-plugin
 */
class Connector {
  constructor({
    debug,
    pipelineId,
    secretId,
    timeout = Number(process.env.SECRETSMGR_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    additionalClientOpts = {},
  }) {
    this.debug = /* istanbul ignore next */ (msg) => debug('%j', msg);
    this.secretId = secretId;
    this.client = Connector.getClient(pipelineId, debug, timeout, additionalClientOpts);
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new SecretsManagerClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
          ...addlRequestHandlerOpts,
        }),
        logger: defaultDebugLogger(debug),
        ...addlClientOpts,
      });
    }
    return this.clients[pipelineId];
  }

  async get() {
    if (!this.secrets) {
      const params = {
        SecretId: this.secretId,
      };

      this.secrets = await Promise.resolve(this.client.send(new GetSecretValueCommand(params)))
        .tapCatch(this.debug)
        .then((data) => Buffer.from(data.SecretString, 'base64').toString())
        .then((data) => JSON.parse(data));
    }

    return this.secrets;
  }
}

export default Connector;
