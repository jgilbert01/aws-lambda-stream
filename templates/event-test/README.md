# event-test

This project provides functions to test that the event hub is setup properly.

## Functions
* publish - an event to the bus
* listener - subscribes to bus
* listener2 - subscribes to stream

## Steps...
1. `sls create --template-url https://github.com/jgilbert01/aws-lambda-stream/tree/master/templates/event-test --path myprefix-event-test`
2. `cd myprefix-event-test`
3. `npm i`
4. `npm test -- -s stg`
5. `npm run dp:lcl -- -s stg`
6. `sls invoke -r us-east-1 -f publish -s stg -d '{"type":"thing-created"}'`
7. `sls logs -f listener -r us-east-1 -s stg`
8. `sls logs -f listener2 -r us-east-1 -s stg`
9. `npm run rm:lcl -- -s stg`
