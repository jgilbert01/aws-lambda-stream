/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { config, SecretsManager } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

/**
 * All secrets are combined to reduce the number of calls
 * Secrets are transferred from a ci/cd pipeline's secured variables
 * to SecretsManager using the serverless-secrets-mgr-plugin
 */
class Connector {
  constructor({
    debug,
    secretId,
    timeout = Number(process.env.SECRETSMGR_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = /* istanbul ignore next */ (msg) => debug('%j', msg);
    this.secretId = secretId;
    this.sm = new SecretsManager({
      httpOptions: {
        timeout,
        connectTimeout: timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  async get() {
    if (!this.secrets) {
      const params = {
        SecretId: this.secretId,
      };

      this.secrets = await this.sm.getSecretValue(params).promise()
        // .tap(this.debug) // *** DO NOT LOGGING SECRETS ***
        .tapCatch(this.debug)
        .then((data) => Buffer.from(data.SecretString, 'base64').toString())
        .then((data) => JSON.parse(data));
    }

    return this.secrets;
  }
}

export default Connector;
