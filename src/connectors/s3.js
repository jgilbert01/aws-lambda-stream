/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
// import { S3, config } from 'aws-sdk';
import { Readable } from 'stream';
import {
  CopyObjectCommand,
  DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client,
} from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import Promise from 'bluebird';
import { omit, pick } from 'lodash';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    bucketName = process.env.BUCKET_NAME,
    timeout = Number(process.env.S3_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
    credentials,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.bucketName = bucketName || 'undefined';
    this.client = Connector.getClient(pipelineId, debug, timeout, credentials, additionalClientOpts);
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, credentials, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new S3Client({
        credentials,
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

  putObject(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new PutObjectCommand(params), ctx);
  }

  deleteObject(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new DeleteObjectCommand(params), ctx);
  }

  getObject(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new GetObjectCommand(params), ctx)
      .then(async (response) => ({ ...response, Body: await response.Body.transformToString() }));
  }

  getObjectStream(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new GetObjectCommand(params), ctx)
      .then((response) => Readable.from(response.Body));
  }

  listObjects(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new ListObjectsV2Command(params), ctx);
  }

  copyObject(inputParams, ctx) {
    const params = {
      Bucket: this.bucketName,
      ...inputParams,
    };

    return this._sendCommand(new CopyObjectCommand(params), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 's3', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
