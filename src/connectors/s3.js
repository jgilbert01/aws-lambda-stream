/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
// import { S3, config } from 'aws-sdk';
import { Readable } from 'stream';
import {
  DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client,
} from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    bucketName = process.env.BUCKET_NAME,
    timeout = Number(process.env.S3_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    xrayEnabled = false,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bucketName = bucketName || 'undefined';
    this.bucket = new S3Client({
      requestHandler: new NodeHttpHandler({
        requestTimeout: timeout,
        connectionTimeout: timeout,
      }),
      logger: defaultDebugLogger(debug),
    });
    if (xrayEnabled) this.bucket = require('../utils/xray').captureSdkClientTraces(this.bucket);
  }

  putObject(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new PutObjectCommand(params));
  }

  deleteObject(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new DeleteObjectCommand(params));
  }

  getObject(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new GetObjectCommand(params))
      .then(async (response) => ({ ...response, Body: await response.Body.transformToString() }));
  }

  getObjectStream(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new GetObjectCommand(params))
      .then((response) => Readable.from(response.Body));
  }

  listObjects(inputParams) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new ListObjectsV2Command(params));
  }

  _sendCommand(command) {
    return Promise.resolve(this.bucket.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
