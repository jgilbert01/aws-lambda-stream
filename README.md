# aws-lambda-stream

**_Create stream processors with AWS Lambda functions._**

The event signature for many Lambda functions is an array containing a micro-batch of `event.Records`. Functional Reactive Programming (FRP) is the cleanest approach for processing these streams. This library provides a light-weight framework for creating these stream processors. The underlying streaming library is [Highland.js](https://highlandjs.org), replete with features like filter, map, reduce, backpressure, batching, parallel processing and more.

Support is provided for AWS Kinesis, DynamoDB Streams and more.

>Note: The original code base has been used in production for years. This specific open-sourced version is in BETA while the various features are ported and documented.

## Install

`npm install aws-lambda-stream --save`

## Basic Usage
The following examples show how to implement basic handler functions for consuming events from a Kinesis stream and a DynamoDB Stream. A key thing to note is that the code you see here is just the initialization code that quickly sets up the steps in the stream pipeline. The final step, `toPromise` returns a Promise from the handler function. Then the promise starts consuming from the stream and the data starts flowing through the steps. The data is pulled through the stream, which provides natural _backpressure (see blow)_. The promise will resolve only once all the data is passed through all the stream steps.

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
* `type` - generally the namespace, entity, action performed
* `timestamp` - epoch value when the action was performed
* `partitionKey` - generally the entity id or correlation id to ensure related events can be processed together
* `tags` - A generic place for routing information. A standard set of values is always included, such as `account`, `region`, `stage`, `source`, `functionname` and `pipeline`.
* `<entity>` - a canonical entity that is specific to the event type. This is the _contract_ that must be held backwards comaptible. The name of this field is usually the lowerCamelCase name of the entity type, such as `thing` for `Thing`.
* `raw` - This is the raw data and format produced by the source of the event. This is included so that the _event-lake_ can form a complete audit with no lost information. This is not guaranteed to be backwards compatible, so use at your own risk.
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

These connectors are then wrapped with utility functions, such as `update` and `publish`, to integrate them into the streaming framework. For example, the returned promise are normalized to [stream](https://highlandjs.org/#_(source)), fault handling is provided and features such as [parallel](https://highlandjs.org/#parallel) and [batch](https://highlandjs.org/#batch) are utilized.

These utility function leverage _currying_ to override default configuration settings, such as the _batchSize_ and the number of _parallel_ asyn-non-blocking_io executions.

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

>Note that I plan to open-source a `fault-monitor` service and the `aws-lambda-stream-cli`. The monitor stores the fault events in S3.  The `cli` supports `resubmitting` the poison events to the function that raised the `fault`.

## Pipelines
TODO

```javascript
import { initialize, execute, fromKinesis } from 'aws-lambda-stream';

import pipeline1 from './pipeline1';
import pipeline2 from './pipeline2';

const PIPELINES = {
  pipeline1,
  pipeline2,
};

export const handler = async (event) => {
  initialize(PIPELINES);

  return execute(fromKinesis(event))
    .through(toPromise);
};
```

Here is an example of a pipeline. They are functions that receive options and then a forked stream as input and add the desired steps. Pipelines typically start with a `filter` step.

```javascript
const pipeline1 = (opt) => (s) => s
  .filter(onEventType)
  .tap(uow => opt.debug('%j', uow))
  .map(toUpdateRequest)
  .through(update({ parallel: 4 }));

export default pipeline1;
```

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
* `pipeline` - the function that imlements the pipeline flavor
* `eventType` - a regex, string or array of strings used to filter on event type
* `toUpdateRequest` - is mapping function exepected by the `materialize` pipeline flavor

## Logging
The [debug](https://www.npmjs.com/package/debug) library is used for logging. When using pipelines, each pipeline is given its own instance and it is passed in with the pipeline configuration options and it is attached to the `uow` for easy access. They are named after the pipelines with a `pl:` prefix. 

This turns on debug for all pipelines.

`cli> DEBUG=pl:*`

This turns on debug for a specific pipeline.

`cli> DEBUG=pl:pipeline2`

Various print utilities are provided, such as: `printStartPipeline` and `printEndPipeline`.

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
