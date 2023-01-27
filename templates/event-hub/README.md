# event-hub

This stack owns all the resources for the subsystems's event-hub, such as the EventBridge bus and Kinesis streams.

The Event Hub is used to create autonomous services that are able to continue operating even when related services are down. Upstream autonomous services produce events to the fully managed Event Hub as their state changes and downstream autonomous services consume events and cache needed data in materialized views.

<img src="overview.png" width="700">


## Further Details
- [Combining the Best of AWS EventBridge and AWS Kinesis](https://medium.com/@jgilbert001/combining-the-best-of-aws-eventbridge-and-aws-kinesis-9b363b043ade)

- [Connecting Accounts with AWS EventBridge](https://medium.com/@jgilbert001/connecting-accounts-with-aws-event-bridge-db89b9e4b697)

- [Stream Inversion & Topology](https://medium.com/@jgilbert001/stream-inversion-topology-ad773627a435)

- [Creating Stream Processors with AWS Lambda Functions](https://medium.com/@jgilbert001/creating-stream-processors-with-aws-lambda-functions-ba1c5da233a3)

- [Template](https://github.com/jgilbert01/aws-lambda-stream/tree/master/templates/event-hub)