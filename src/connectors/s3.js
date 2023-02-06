/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { S3, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    bucketName = process.env.BUCKET_NAME,
    timeout = Number(process.env.S3_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bucketName = bucketName || 'undefined';
    this.bucket = new S3({
      httpOptions: {
        timeout,
        connectTimeout: timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  putObject(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this.bucket.putObject(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }

  getObject(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this.bucket.getObject(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }

  listObjects(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this.bucket.listObjectsV2(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
