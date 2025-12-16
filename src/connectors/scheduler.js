/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import {
  CreateScheduleCommand,
  SchedulerClient,
  ActionAfterCompletion,
} from '@aws-sdk/client-scheduler';
import Promise from 'bluebird';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { ConfiguredRetryStrategy } from '@smithy/util-retry';
import { omit, pick } from 'lodash';
import { defaultRetryConfig, defaultBackoffDelay } from '../utils/retry';
import { defaultDebugLogger } from '../utils/log';

class Connector {
  constructor({
    debug,
    pipelineId,
    timeout = Number(process.env.SCHEDULER_TIMEOUT)
    || Number(process.env.TIMEOUT)
    || 1000,
    busArn = process.env.BUS_ARN,
    roleArn = process.env.SCHEDULER_ROLE_ARN,
    kmsKeyArn = process.env.MASTER_KEY_ARN,
    retryConfig = defaultRetryConfig,
    additionalClientOpts = {},
    ...opt
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.client = Connector.getClient(pipelineId, debug, timeout, additionalClientOpts);
    this.busArn = busArn;
    this.roleArn = roleArn;
    this.kmsKeyArn = kmsKeyArn;
    this.retryConfig = retryConfig;
    this.opt = opt;
  }

  static clients = {};

  static getClient(pipelineId, debug, timeout, additionalClientOpts) {
    const addlRequestHandlerOpts = pick(additionalClientOpts, ['requestHandler']);
    const addlClientOpts = omit(additionalClientOpts, ['requestHandler']);

    if (!this.clients[pipelineId]) {
      this.clients[pipelineId] = new SchedulerClient({
        requestHandler: new NodeHttpHandler({
          requestTimeout: timeout,
          connectionTimeout: timeout,
          ...addlRequestHandlerOpts,
        }),
        retryStrategy: new ConfiguredRetryStrategy(11, defaultBackoffDelay),
        logger: defaultDebugLogger(debug),
        ...addlClientOpts,
      });
    }
    return this.clients[pipelineId];
  }

  schedule(inputParams, ctx) {
    const params = {
      ...inputParams,
      Target: {
        ...inputParams.Target,
        Arn: inputParams.Target.Arn || this.busArn,
        RoleArn: inputParams.Target.RoleArn || this.roleArn,
      },
      ActionAfterCompletion: ActionAfterCompletion.DELETE,
      KmsKeyArn: params.KmsKeyArn || this.kmsKeyArn,
    };

    return this._sendCommand(new CreateScheduleCommand(params), ctx);
  }

  _sendCommand(command, ctx) {
    this.opt.metrics?.capture(this.client, command, 'scheduler', this.opt, ctx);
    return Promise.resolve(this.client.send(command))
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
