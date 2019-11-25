/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { Kinesis, config } from 'aws-sdk';
import Promise from 'bluebird';

config.setPromisesDependency(Promise);

class Connector {
  constructor(debug, streamName = process.env.STREAM_NAME, timeout = process.env.KINESIS_TIMEOUT || process.env.TIMEOUT || 1000) {
    this.debug = debug;
    this.streamName = streamName || 'undefined';
    this.stream = new Kinesis({
      httpOptions: {
        timeout,
        // agent: sslAgent,
      },
      logger: { log: /* istanbul ignore next */ (msg) => this.debug(msg) },
    });
  }

  publish(events) {
    const params = {
      StreamName: this.streamName,
      Records: events.map((e) => ({
        Data: Buffer.from(JSON.stringify(e)),
        PartitionKey: e.partitionKey,
      })),
    };

    return this.stream.putRecords(params).promise()
      .tap(this.debug)
      .tapCatch(this.debug);
  }
}

export default Connector;
