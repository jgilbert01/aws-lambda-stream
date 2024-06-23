import {
  createEventSourceMapping,
  deleteEventSourceMapping,
  updateEventSourceMapping,
} from '../sinks/lambda';
import {
  listEventSourceMappings,
} from '../queries/lambda';
import {
  faulty, printEndPipeline,
} from '../utils';

// primary/secondary failover for kinesis stream based listener functions
// move the event source mappings between regions
// failover when uow.event.detail.state === 'ALARM'
// failback when uow.event.detail.state === 'OK'
// forward alarm events to other region
// consume alarm event in a healthly region and perform the switch

export const streamFailover = (opt) => (s) => s // eslint-disable-line import/prefer-default-export
  .filter((uow) =>
    // process fail-over in the secondary region
    (uow.event.detail.state.value === 'ALARM' && uow.event.region === process.env.FAILOVER_REGION)
    // process fail-back in the primary region
    || (uow.event.detail.state.value === 'OK' && uow.event.region === process.env.AWS_REGION))

  // get esm info from other region
  .map(toListRequest(opt))
  .through(listEventSourceMappings(opt))

  // create esm in current region
  .map(toCreateRequest(opt))
  .through(createEventSourceMapping(opt))

  // now it is safe to delete esm in other region
  .map(toDeleteRequest(opt))
  .through(deleteEventSourceMapping(opt))

  // finally enable in current region
  .map(toUpdateRequest(opt))
  .through(updateEventSourceMapping(opt))

  .tap(printEndPipeline);

const toListRequest = (opt) => faulty((uow) => ({
  ...uow,
  listRequest: {
    region: process.env.FAILOVER_REGION, // toggle region
    FunctionName: process.env.LISTENER_FUNCTION_NAME, // Ref: ListenerLambdaFunction
  },
}));

const subtract = (time, amount) => {
  const d = new Date(time);
  return d.setMinutes(d.getMinutes() - amount);
};

const toCreateRequest = (opt) => faulty((uow) => ({
  ...uow,
  createRequest: uow.listResponse.EventSourceMappings[0]?.Enabled ? {
    region: process.env.AWS_REGION, // toggle region
    ...uow.listResponse.EventSourceMappings[0], // copy properties from other region
    UUID: undefined,
    EventSourceArn: process.env.STREAM_ARN,
    FunctionName: process.env.LISTENER_FUNCTION_NAME, // Ref: ListenerLambdaFunction
    StartingPosition: 'AT_TIMESTAMP',
    StartingPositionTimestamp: subtract(uow.event.time, Number(process.env.BACKOFF || 30)),
    Enabled: false,
  } : /* istanbul ignore next */ undefined,
}));

const toDeleteRequest = (opt) => faulty((uow) => ({
  ...uow,
  deleteRequest: uow.createResponse && uow.listResponse.EventSourceMappings[0] ? {
    region: process.env.FAILOVER_REGION, // toggle region
    UUID: uow.listResponse.EventSourceMappings[0].UUID,
  } : /* istanbul ignore next */ undefined,
}));

const toUpdateRequest = (opt) => faulty((uow) => ({
  ...uow,
  updateRequest: uow.deleteResponse && uow.createResponse?.UUID ? {
    region: process.env.AWS_REGION, // toggle region
    UUID: uow.createResponse.UUID,
    Enabled: true,
  } : /* istanbul ignore next */ undefined,
}));
