const _ = require('highland');

module.exports.listener = (event, context, cb) => {
  console.log('event: %j', event);

  _(event.Records)
    .map(recordToUow)
    .tap(printUow)
    .filter(forData)
    .map(toEvent)
    .tap(printUow)
    .collect()
    .tap(printCount)
    .toCallback(cb);
};

const recordToUow = r => ({
  record: r,
  event: JSON.parse(Buffer.from(r.kinesis.data, 'base64'))
});

const printUow = uow => console.log('uow: %j', uow);

const forData = uow => uow.event.metadata['record-type'] === 'data';

const printCount = events => console.log('count: %d', events.length);

const toEvent = (uow) => ({
  ...uow,
  event: {
    id: uow.record.eventID,
    // id: uow.event.metadata['transaction-id'] || uow.record.eventID,
    type: `${uow.event.metadata['table-name'].toLowerCase()}-${uow.event.metadata.operation}`,
    timestamp: (new Date(uow.event.metadata.timestamp)).getTime(),
    partitionKey: uow.record.kinesis.partitionKey,
    // tags: {

    // },
    raw: uow.event,

  }
})

// TODO - id, type-suffix, tags, latching, resync, pk, before/after?
// Dynamo ? pk = table name, sk = id, gsk = parent?

/**

{
    "data": {
        "ID": 8,
        "FNAME": "John8",
        "LNAME": "G8"
    },
    "metadata": {
        "timestamp": "2021-01-08T18:34:14.440377Z",
        "record-type": "data",
        "operation": "insert|update|delete",
        "partition-key-type": "schema-table",
        "schema-name": "public",
        "table-name": "PERSON",
        "transaction-id": 15891
    }
}



{
    "record": {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "public.PERSON",
            "sequenceNumber": "49614369230397162492610837642244914591420813857098039298",
            "data": "ewoJImRhdGEiOgl7CgkJIklEIjoJMwoJfSwKCSJtZXRhZGF0YSI6CXsKCQkidGltZXN0YW1wIjoJIjIwMjEtMDEtMDhUMTg6NTk6MjYuOTE4NTcwWiIsCgkJInJlY29yZC10eXBlIjoJImRhdGEiLAoJCSJvcGVyYXRpb24iOgkiZGVsZXRlIiwKCQkicGFydGl0aW9uLWtleS10eXBlIjoJInNjaGVtYS10YWJsZSIsCgkJInNjaGVtYS1uYW1lIjoJInB1YmxpYyIsCgkJInRhYmxlLW5hbWUiOgkiUEVSU09OIiwKCQkidHJhbnNhY3Rpb24taWQiOgkxNTg5OQoJfQp9",
            "approximateArrivalTimestamp": 1610132366.923
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": "shardId-000000000000:49614369230397162492610837642244914591420813857098039298",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::026257137139:role/dms-gateway-stg-us-east-1-lambdaRole",
        "awsRegion": "us-east-1",
        "eventSourceARN": "arn:aws:kinesis:us-east-1:026257137139:stream/dms-gateway-stg-s1"
    },
    "event": {
        "id": "shardId-000000000000:49614369230397162492610837642244914591420813857098039298",
        "type": "person-deleted",
        "timestamp": 1610132366918,
        "partitionKey": "public.PERSON",
        "raw": {
            "data": {
                "ID": 8,
                "FNAME": "John8",
                "LNAME": "G8"
            },
            "metadata": {
                "timestamp": "2021-01-08T18:59:26.918570Z",
                "record-type": "data",
                "operation": "delete",
                "partition-key-type": "schema-table",
                "schema-name": "public",
                "table-name": "PERSON",
                "transaction-id": 15899
            }
        }
    }
}

 */
