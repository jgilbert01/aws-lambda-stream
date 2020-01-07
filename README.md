# aws-lambda-stream

**_Create stream processors with AWS Lambda functions._**

The event signature for many Lambda functions is an array containing a micro-batch of `event.Records`. Functional Reactive Programming (FRP) is the cleanest approach for processing these streams. This library provides a light-weight framework for creating these stream processors. The underlying streaming library is [Highland.js](https://highlandjs.org), replete with features like filter, map, reduce, backpressure, batching, parallel processing and more.

Support is provided for AWS Kinesis, DynamoDB Streams and more.

>Note: The original code base has been used in production for years. This specific open-sourced version is in BETA while the various features are ported and documented.

## Install

`npm install aws-lambda-stream --save`

## Basic Usage
The following examples show how to implement basic handler functions for consuming events from a Kinesis stream and a DynamoDB Stream. A key thing to note is that the code you see here is responsible for assembling the steps in the stream pipeline. The final step, `toPromise` returns a Promise from the handler function. Then the promise starts consuming from the stream and the data starts flowing through the steps. The data is pulled through the steps, which provides natural _backpressure (see blow)_. The promise will resolve once all the data has passed through all the stream steps or reject when an unhandled error is encountered.

### Example: Listener Function
This example processes a Kinesis stream and materializes the data in a single DynamoDB table. The details are explained below.

```javascript
import { fromKinesis, toPromise } from 'aws-lambda-stream';

export const handler = async (event) =>
  fromKinesis(event)
    .filter(onEventType)
    .map(toUpdateRequest)
    .through(update({ parallel: 4 }))
    .through(toPromise);
```

### Example: Trigger Function
This example processes a DynamoDB Stream and publishes CUD events to a Kinesis stream. The details are explained below.

```javascript
import { fromDynamodb, toPromise } from 'aws-lambda-stream';

export const handler = async (event) =>
  fromDynamodb(event)
    .map(toEvent)
    .through(publish({ batchSize: 25 }))
    .through(toPromise);
```

## Creating a stream from a lambda event
The first step of a stream processor transforms the incoming Records into a [stream](https://highlandjs.org/#_(source)), like such: `_(event.Records)`. The various `from` functions, such as `fromKinesis` and `fromDynamodb`, normialize the records into a standard `Event` format. The output is a stream of `UnitOfWork` objects.

## UnitOfWork Type (aka uow)
Think of a `uow` as an _immutable_ object that represents the `scope` of a set of `variables` passing through the stream. More so than ever, we should not use global variables in stream processors. Your processor steps will add new variables to the `uow` for use by downstream steps (see _Mapping_ below). This _scoping_ is crucial when we leverage the parallel processing features discussed below.

```javascript
interface UnitOfWork {
  record: any;
  event?: Event;
  batch?: UnitOfWork[];
}
```

* `record` - the original record
* `event` - the standardized event
* `batch` - an array of uow that should succeed or fail together (see _Batching_ and _Grouping_ below)

## Event Type
The various streaming and messaging channels each have their own formats. We want to decouple the processoring logic from the choice of these technologies. Thus all published events conform to the following `Event` format. This also provides for polymorphic-like processing. This standard format is also leveraged in the _event-lake_ and _micro-event-store_ features.

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
* `type` - generally the namespace, domain entity and action performed
* `timestamp` - epoch value when the action was performed
* `partitionKey` - generally the entity id or correlation id to ensure related events can be processed together
* `tags` - a generic place for routing information. A standard set of values is always included, such as `account`, `region`, `stage`, `source`, `functionname` and `pipeline`.
* `<entity>` - a canonical entity that is specific to the event type. This is the _contract_ that must be held backwards compatible. The name of this field is usually the lowerCamelCase name of the entity type, such as `thing` for `Thing`.
* `raw` - this is the raw data and format produced by the source of the event. This is included so that the _event-lake_ can form a complete audit with no lost information. This is not guaranteed to be backwards compatible, so use at your own risk.
* `encryptionInfo` - envelope encryption metadata (see _Encryption_ below)

## Filters
For a variety of reasons, we generally multiplex many event types through the same stream. I discuss this in detail in the following post: [Stream Inversion & Topology](https://medium.com/@jgilbert001/stream-inversion-topology-ad773627a435?source=friends_link&sk=a3639a9f8d459dd60266569380fb5c71). Thus, we use `filter` steps with functions like `onEventType` to focus in on the event types of interest and perform content based routing in general.

```javascript
const onEventType = event => event.type.match(/thing-*/); // all event types starting with `thing-`
```

## Mapping
Many stream processor steps map the incoming data to the format needed downstream. The results of the mapping are adorned to the `uow` as a new variable. The `uow` must be immutable, so we return a new `uow` by cloning the original `uow` with the _spread_ operator and adorning the additional variable. There are various utils provided to assist (see below).

```javascript
.map((uow) => ({
  ...uow,
  variableName: {
    // mapping logic here
  }
}))
```

This is the function used in the _Listener Function_ example above.

```javascript
const toUpdateRequest = (uow) => ({
  ...uow,
  updateRequest: { // variable expected by `update` util
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

This is the function used in the _Trigger Function_ example above.

```javascript
const toEvent = (uow) => ({
  ...uow,
  event: { // variable expected by the `publish` util
    ...event,
    thing: uow.event.raw.new, // canonical
  }
});
```

> Note: It is best to perform mapping in a separate upstream step from the step that will perform the async-non-blocking-io to help maximize the potential for concurrent processing. (aka cooperative programming)

## Connectors
At the end of a stream processor there is usually a _sink_ step that persists the results to a datastore or another stream. These external calls are wrapped in thin `Connector` classes so that they can be easily _mocked_ for unit testing.

These connectors are then wrapped with utility functions, such as `update` and `publish`, to integrate them into the streaming framework. For example, the promise returned from the connector is normalized to a [stream](https://highlandjs.org/#_(source)), fault handling is provided and features such as [parallel](https://highlandjs.org/#parallel) and [batch](https://highlandjs.org/#batch) are utilized.

These utility function leverage _currying_ to override default configuration settings, such as the _batchSize_ and the number of _parallel_ asyn-non-blocking-io executions.

Here is the example of using the `update` function.

```javascript
import { update, toPromise } from 'aws-lambda-stream';
  ...
  .through(update({ parallel: 4 }))
  .through(toPromise);
```

Here is the example of using the `publish` function.

```javascript
import { publish, toPromise } from 'aws-lambda-stream';
  ...
  .through(publish({ batchSize: 25 }))
  .through(toPromise);
```

## Faults
When there is an unhandled error in a Kinesis stream processor, Lambda will continuously retry the function until the problem is either resolved or the event(s) in question expire(s). For transient errors, such as throttling, this may be the best course of action, because the problem may self-heal. However, if there is a poison event then we want to set it asside by publishing a `fault` event, so that the other events can be processed. I refer to this as the _Stream Circuit Breaker_ pattern.

Here is the definition of a `fault` event.

```javascript
export const FAULT_EVENT_TYPE: string = 'fault';

interface FaultEvent extends Event {
  err: {
    name: string;
    message: string;
    stack: string;
  };
  uow: UnitOfWork;
}
```

* `err` - contains the error information
* `uow` - contains the state of the variables in the `uow` when the error happened

When an error is thrown in a _Highland.js_ stream, the error will skip over all the remaining steps until it is either caught by an [errors](https://highlandjs.org/#errors) step or it reaches the end of the stream and all processing stops with the error.

When you want to handle a poison event and raise a `fault` event then simply catch the error, adorn the current `uow` to the error and rethrow the error. Several utilities are provided to assist: `throwFault` for standard try/catch, `rejectWithFault` for promises, and `faulty` and `faultyAsync` are function wrappers.

Here is an example of using `throwFault`.

```javascript
try {
  ...
} catch (err) {
  throwFault(err);
}

export const throwFault = (uow) => (err) => {
  // adorn the troubled uow
  // for processing in the errors handler
  err.uow = uow;
  throw err;
};
```

Then we need to setup the `faults` errors function and the `flushFaults` stream. _Fault handling is already included when using the `pipelines` feature (see below)._

```javascript
import { faults, flushFaults, toPromise } from 'aws-lambda-stream';
  ...
  .errors(faults)
  .through(flushFaults)
  .through(toPromise);
```

The `faults` function tests to see if the `err` has a `uow` adorned. If so then it buffers a `fault` event. The `flushFaults` stream will published all the buffered `fault` events once all events in the batch have been processed. This ensures that the `fault` events are not prematurely published in case an unhandle error occurs later in the batch.

>I plan to open source a `fault-monitor` service and the `aws-lambda-stream-cli`. The monitor stores the fault events in S3.  The `cli` supports `resubmitting` the poison events to the function that raised the `fault`.

## Pipelines
As mentioned above, we are multiplexing many event types through a single stream for a variety of good reasons. Therefore, we want to maximize the utilization of each function invocation by acting on as many events as possible. However, we also want to maintain good clean separation of the processing logic for these different event types. 

The _Highland.js_ library allows us to [fork](https://highlandjs.org/#observe) streams, passing each fork/observer through a [pipeline](https://highlandjs.org/#pipeline) and [merge](https://highlandjs.org/#merge) the streams back together where they can share common tail logic like `fault` handling.

Each pipeline is implemented and tested separately. Each is usually defined in its own module/file.

Here is an example of a pipeline. They are _curried_ functions that first receive options during `initialize` and then the forked stream during `assemble` (see below). During `assemble` they add the desired steps to the stream. Pipelines typically start with one or more `filter` steps to indicate which events the steps apply to.

```javascript
const pipeline1 = (options) => (stream) => stream
  .filter(onEventType)
  .tap(uow => options.debug('%j', uow))
  .map(toUpdateRequest)
  .through(update({ parallel: 4 }));

export default pipeline1;
```

Here is an example of a handler function that uses pipelines. 
1. First we `initialize` the pipelines with any options. 
2. Then we `assemble` all pipelines into a forked stream.
3. And finally the processing of the events through the pipelines is started by `toPromise`. 
4. The data fans out through all the pipelines and the processing concludes when all the units of work have flowed through and merged back together.

```javascript
import { initialize, fromKinesis } from 'aws-lambda-stream';

import pipeline1 from './pipeline1';
import pipeline2 from './pipeline2';

const PIPELINES = {
  pipeline1,
  pipeline2,
};

const OPTIONS = { ... };

export const handler = async (event) => 
  initialize(PIPELINES, OPTIONS)
    .assemble(fromKinesis(event))
    .through(toPromise);
```

But take care to assemble a cohesive set of pipelines into a single function. For example, a _listener_ function in a BFF service will typically consume events from Kinesis and the various pipelines will `materialize` different entities from the events into a DynamoDB table to implement the _CQRS_ pattern. Then the _trigger_ function of the BFF service will consume events from the DynamoDB table, as `mutations` are invoked in the `graphql` function, and these pipelines will `publish` events to the Kinesis stream to implement the _Event Sourcing_ pattern. (see Flavors below)

>Pipelines also help optimize utilization by giving a function more things to do while it waits on async-non-blocking-io calls (see Parallel below). Run `test/unit/pipelines/coop.test.js` to see an example of _cooperative programming_ in action.

## Flavors
Many of the pipelines we write follow the exact same steps and only the filters and data mapping details are different. We can package these pipeline _flavors_ into reusable pipelines that can be configured with `rules`.

The following _flavors_ are included and you can package your own into libaries.
* `materialize` - used in `listener` functions to materialize an `entity` from an `event` into a DynamoDB single table
* `crud` - used in `trigger` functions to `publish` events to Kinesis as entities are maintained in a DynamoDB single table
* more to be ported soon

Here is an example of initializing pipelines from rules. Note that you can initialize one-off pipelines along side rule-driven pipelines.

```javascript
import { initializeFrom } from 'aws-lambda-stream';

const PIPELINES = {
  pipeline1,
  pipeline2,
  ...initializeFrom(RULES),
};
```

Here are some example rules. The `id`, `pipeline`, and `eventType` fields are required. The remaining fields are defined by the specified pipeline flavor. You can define functions inline, but it is best to implement and unit test them separately.

```javascript
import materialize from 'aws-lambda-stream/flavors/materialize';

const RULES = [
  {
    id: 'p1',
    pipeline: materialize,
    eventType: /thing-(created|updated)/,
    toUpdateRequest,
  },
  {
    id: 'p2',
    pipeline: materialize,
    eventType: 'thing-deleted',
    toUpdateRequest: toSoftDeleteUpdateRequest,
  },
  {
    id: 'p3',
    pipeline: materialize,
    eventType: ['something-created', 'something-updated'],
    toUpdateRequest: (uow) => ({ ... }),
  },
];
```
* `id` - is a unqiue string
* `pipeline` - the function that implements the pipeline flavor
* `eventType` - a regex, string or array of strings used to filter on event type
* `toUpdateRequest` - is a mapping function expected by the `materialize` pipeline flavor

## Logging
The [debug](https://www.npmjs.com/package/debug) library is used for logging. When using pipelines, each pipeline is given its own instance and it is passed in with the pipeline configuration options and it is attached to the `uow` for easy access. They are named after the pipelines with a `pl:` prefix. 

This turns on debug for all pipelines.

`cli> DEBUG=pl:*`

This turns on debug for a specific pipeline.

`cli> DEBUG=pl:pipeline2`

Various print utilities are provided, such as: `printStartPipeline` and `printEndPipeline`.

## Utilities
Here are some highlights of utiltities that are available in this library or Highland.js.

### Backpressure
Unlike imperative programming, functional reactive programming with streams provides natural backpressure because it is pull oriented. In other words, a slow downstream step will not pull the next upstream record until it is finished processing the current record. This helps us avoid overwhelming downstream services and systems.

However, this does not hold true for services like DynamoDB that return throttling errors. In these cases we can use the Highland.js [rateLimit](https://highlandjs.org/#ratelimit) feature to provide explicit backpressure.

```javascript
  ...
  .rateLimit(2, 100) // 2 per 100ms
  .through(update)
  ...
```

### Parallel
Asynchronous Non Blocking IO is probably the most important feature for optimizing throughput. The Highland.js [parallel](https://highlandjs.org/#parallel) feature allows us to take full control. When using this feature, upstream steps will continue to be executed while up to N asyc requests are waiting for responses. This feature along with `pipelines` allows us to maximize the utilization of every lambda invocation. 

```javascript
  ...
  .map(makeSomeAsyncCall)
  .parallel(8)
  ...
```

This is usually the first parameter I tweak when tuning a function. Environment variables, such as `UPDATE_PARALLEL` and `PARALLEL` are used for experimenting with different settings. 

>Here is a post on _queuing theory_ that helps put this in perspective: [What happens when you add another teller?](https://www.johndcook.com/blog/2008/10/21/what-happens-when-you-add-a-new-teller)

This feature is baked into the DynamoDB `update` and Kinesis `publish` utilities. 

### Batching
Many `aws-sdk` operations support batching multiple requests into a single call. This can help increase throughput by reducing aggregate network latency.

The Highland.js [batch](https://highlandjs.org/#batch) feature allows us to easily collect us a batch of requests. The `toBatchUow` utility provided by this library formats these into a batch unit of work so that we can easily raise a `fault` for a batch and `resubmit` the batch.

```javascript
  ...
  .batch(10)
  .map(toBatchUow)
  .map(makeSOmeAsyncCall)
  ...
```

However, be aware that most of the aws-sdk batch apis do not succeed or fail as a unit. Therefore you either have to selectively retry the failed requests and/or ensure that these calls are idempotent. Therefore I usually try to first optimize using the `parellel` feature and then move onto `batch` if needs be.

_I will look at adding selective retry as a feature of this library._

### Grouping
Another way to increase throughput is by grouping related events and thereby reducing the number external calls you will need to make. The Highland.js [group](https://highlandjs.org/#group) feature allows us to easily group related records.  The `toGroupUows` utility provided by this library formats these into batched units of work so that we can easily raise a `fault` for a group and `resubmit` the group.

```javascript
  ...
  .group(uow => uow.event.partitionKey)
  .flatMap(toGroupUows)
  ...
```

## Other
There are various other utilities in the utils folder.
* `now` - wraps `Date.now()` so that it can be easily mocked in unit tests
* `toKinesisRecords` - is a test helper for creating Kinesis records from test events
* `toDynamodbRecords` - is a test helper for creating DynamoDN Strean records from test events

## Kinesis Support
* `fromKinesis` - creates a stream from Kinesis records
* `Publisher` - connector for the Kinesis SDK
* `publish` - stream steps for publishing events to Kinesis
* `toKinesisRecords` - test helper mentioned above

## DynamoDB Support
* `fromDynamodb` - creates a stream from DynamoDB Stream records
* `DynamoDBConnector` - connector for the DynamoDB SDK
* `update` - stream steps for updating rows in a single DynamoDB table
* `toDynamodbRecords` - test helper mentioned above
* `updateExpression` - creates an expression from a plain old json object
  * see _Mapping_ above
  * _consider using [DynamoDB Toolbox](https://github.com/jeremydaly/dynamodb-toolbox) for richer support_
* `timestampCondition` - creates an expression for performing _inverse oplocks_
* `ttl` - calculates `ttl` based on a start epoch and a number of days

In addition:
* `single table` support is provided in `fromDynamodb` based on the `discriminator` field
* `latching` support is provided in `fromDynamodb` based on the `latched` field
* `soft delete` support is provided in `fromDynamodb` based on the `deleted` field
* `global table` support is provided in `fromDynamodb` based on the `aws:rep:updateregion` field
  * _note this may not be needed in the latest version of global tables_

> Look for future blog posts on `dynamodb single tables`, `latching`, `soft-deletes` and `oplock-based-joins`.

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

## Links
The following links contain additional information:
* [Highland.js](https://highlandjs.org) documentation
* My [Blog](https://medium.com/@jgilbert001) covers many topics such as _System Wide Event Sourcing & CQRS_
