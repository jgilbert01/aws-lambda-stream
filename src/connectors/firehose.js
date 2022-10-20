/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { Firehose, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor({
    debug,
    deliveryStreamName = process.env.DELIVERY_STREAM_NAME,
    timeout = Number(process.env.FIREHOSE_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  }) {
    this.debug = (msg) => debug('%j', msg);
    this.deliveryStreamName = deliveryStreamName || 'undefined';
    this.stream = new Firehose({
      httpOptions: {
        timeout,
      },
      logger: { log: /* istanbul ignore next */ (msg) => debug('%s', msg.replace(/\n/g, '\r')) },
    });
  }

  putRecordBatch(inputParams) {
    const params = {
      DeliveryStreamName: this.deliveryStreamName,
      ...inputParams,
    };

    return this.stream.putRecordBatch(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
