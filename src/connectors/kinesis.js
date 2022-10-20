/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { Kinesis, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    streamName = process.env.STREAM_NAME,
    timeout = Number(process.env.KINESIS_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.streamName = streamName || 'undefined';
    this.stream = new Kinesis({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  putRecords(inputParams) {
    const params = {
      StreamName: this.streamName,
      ...inputParams,
    };

    return this.stream.putRecords(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
