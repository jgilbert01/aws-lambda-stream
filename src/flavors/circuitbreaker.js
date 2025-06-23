import { updateEventSourceMapping } from '../sinks/lambda';
import { faulty } from '../utils';

// typically used for egress gateways
// attached to a CloudWatch alarm
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/AlarmThatSendsEmail.html#alarms-and-actions

export const circuitBreaker = (opt) => (s) => s // eslint-disable-line import/prefer-default-export
  .map(toUpdateRequest(opt))
  .through(updateEventSourceMapping(opt));

const toUpdateRequest = (opt) => faulty((uow) => ({
  ...uow,
  updateRequest: {
    UUID: process.env.ESM_ID, // Ref: TriggerEventSourceMappingDynamodbEntitiesTable
    Enabled: uow.event.alarmData.state.value !== 'ALARM',
    BatchSize: uow.event.alarmData.state.value === 'INSUFFICIENT_DATA'
      ? 1 : Number(opt.circuitBreakerMaxBatchSize)
      || Number(process.env.CIRCUIT_BREAKER_MAX_BATCH_SIZE)
      || Number(process.env.BATCH_SIZE)
      || 100,
  },
}));
