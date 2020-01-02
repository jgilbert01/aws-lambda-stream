# aws-lambda-stream

**_Create stream processors with AWS Lambda functions._**

The event signature for many Lambda functions is an array containing a micro-batch of `event.Records`. Functional Reactive Programming (FRP) is the cleanest approach for processing these streams. This library provides a light-weight framework for creating these stream processors. The underlying streaming library is [Highland.js](https://highlandjs.org), replete with features like filter, map, reduce, backpressure, batchinging, parallel processing and more.

Support is provides for AWS Kinesis, DynamoDB STreams and more.

>Note: The original code base has been used in production for years. This specific open-sourced version is in BETA while the various features are ported and documented.

## Install

`npm install aws-lambda-stream --save`

## Example: Listener Function
This example processes a Kinesis stream and materializes the data in a single DynamoDB table. The details are explained below.

```javascript
export const handler = async (event) =>
      fromKinesis(event)
        .filter(onEventType)
        .map(toUpdateRequest)
        .map(updateTable)
        .parallel(4)
        .through(toPromise);
```

## Example: Trigger Function
This example processes a DynamoDB Stream and publishes CUD events to a Kinesis stream. The details are explained below.

```javascript
export const handler = async (event) =>
  fromDynamodb(event)
    .map(toEvent)
    .batch(25)
    .through(publish)
    .through(toPromise);
```

## Creating a stream from a lambda event
The first step of a stream processor transforms the incoming Records into a [stream](https://highlandjs.org/#_(source)), like such: `_(event.Records)`. The various `from` functions, such as `fromKinesis` and `fromDynamodb`, normialize the records into a standard `Event` format. The output is a stream of `UnitOfWork` objects.

## UnitOfWork Type (aka uow)
Think of a `uow` as an _immutable_ object that represents the `scope` of a set of `variables` passing through the stream. More so than ever, we should not use global variables in stream processors. Your processor steps will add new variables to the `uow` for use by downstream steps (see Mapping below). This _scoping_ is crucial when we leverage the parallel processing features discussed below.

```javascript
interface UnitOfWork {
	record: any;
	event?: Event;
	batch?: UnitOfWork[];
}
```

* `record` - the original record
* `event` - the standardized event
* `batch` - a set of uow that should succeed or fail together (see _Batching_ and _Grouping_ below)

## Event Type
The various streaming and messaging channels each have their own formats. We want to decouple the processoring logic from the choice of these technologies. Thus all published events conform to the following `Event` format. This also provides for polymorphic-like processing. This standard format is also leveraged in a _event-lake_ and _micro-event-stores_.

```javascript
interface Event {
	id: string;
	type: string;
	timestamp: number;
	partitionKey?: string;
	tags: { [key: string]: string | number };
	raw?: any; 
	encryptionInfo?: any;
}
```

* `id` - a unique deterministic value
* `type` - generally the namespace, entity, action performed
* `timestamp` - epoch value when the action was performed
* `partitionKey` - generally the entity id or correlation id to ensure related events can be processed together
* `tags` - A generic place for routing information. A standard set of values is always included, such as `account`, `region`, `stage`, `source`, `functionname` and `pipeline`.
* `<entity>` - a canonical entity that is specific to the event type. This is the _contract_ that must be held backwards comaptible. The name of the field is usually the lowerCamelCase variation of the entity type, such as `thing` for `Thing`.
* `raw` - This is the raw data and format produced by the source of the event. This is included so that the _event-lake_ can form a complete audit with no lost information. This is not guarenteed to be backwards compatible, so use at yoour own risk.
* `encryptionInfo` - envelope encryption metadata (see _Encryption_ below)

## Filters
For a variety of reasons, we generally multiplex many event types through the same stream. I discuss this in detail in the following post: [Stream Inversion & Topology](https://medium.com/@jgilbert001/stream-inversion-topology-ad773627a435?source=friends_link&sk=a3639a9f8d459dd60266569380fb5c71). Thus, we use `filter` steps with functions like `onEventType` to focus in on the event types of interest and perform content based routing in general.

```javascript
const onEventType = event => event.type.match(/thing-*/); // all event types starting with `thing-`
```

## Mapping
Many stream processor steps map the incoming data to the format needed downstream. The results of the mapping are adnorned to the `uow` as a new variable. The `uow` must be immutable, so we return a new `uow` by cloning the original `uow` with the spread operator and adorning the additional variable. The are various utils provided to assit (see below).

```javascript
.map((uow) => ({
    ...uow,
    variableName: {
      // mapping logic here
    }
}))
```

This is the function used in the Listener Function example above.

```javascript
const toEvent = (uow) => ({
  ...uow,
  event: { // variable expected by kinesis util
    ...event,
    thing: uow.event.raw.new, // canonical
  }
});
```

This is the function used in the Trigger Function example above.

```javascript
const toUpdateRequest = (uow) => ({
    ...uow,
    updateRequest: { // variable expected by dynamodb util
      Key: {
        pk: uow.event.thing.id,
        sk: 'thing',
      },
      ...updateExpression({
        ...uow.event.thing,
        discriminator: 'thing',
        timestamp: uow.event.timestamp,
      }),
      ...timestampCondition(),
    }
});
```

> Note: It is best to perform mapping in a separate upstream step from the step that will perform the async-non-blocking-io to help maximize the potential for concurrent processing. (aka cooperative programming)

## Connectors
TODO

## Faults
TODO

## Pipelines
TODO

## Flavors
TODO

## Logging
TODO

## Kinesis Support
TODO

## DynamoDB Support
TODO

## EventBridge Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/18

## S3 Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/17

## SNS Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/15

## SQS Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/16

## Encryption Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/20

## Metrics Support
* https://github.com/jgilbert01/aws-lambda-stream/issues/21

## Validation
* https://github.com/jgilbert01/aws-lambda-stream/issues/22
