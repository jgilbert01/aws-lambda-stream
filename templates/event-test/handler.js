const aws = require('aws-sdk');
const uuid = require('uuid');

exports.publish = async (event, context) => {
  console.log('event: %j', event);
  console.log('context: %j', context);

  const params = {
    Entries: [
      {
        EventBusName: process.env.BUS_NAME,
        Source: 'custom',
        DetailType: event.type,
        Detail: JSON.stringify({
          id: event.id || uuid.v1(),
          partitionKey: event.partitionKey || uuid.v4(),
          timestamp: Date.now(),
          tags: {
            region: process.env.AWS_REGION,
            functionname: process.env.AWS_LAMBDA_FUNCTION_NAME,
          },
          ...event,
        }),
      },
    ]
  };

  console.log('params: %j', params);

  const eventBridge = new aws.EventBridge();

  return eventBridge.putEvents(params).promise();
};

exports.listener = async (event, context) => {
  console.log('event: %j', event);
  console.log('context: %j', context);
};

exports.listener2 = async (event, context) => {
  console.log('event: %j', event);
  console.log('context: %j', context);
  event.Records.forEach((record) => console.log('e: %j', JSON.parse(Buffer.from(record.kinesis.data, 'base64'))))
};
