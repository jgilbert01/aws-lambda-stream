import { updateExpression, timestampCondition } from 'aws-lambda-stream';

import { now, ttl } from '../utils';
import { mapper } from '../connectors/dynamodb';

const DISCRIMINATOR = 'thing';

const MAPPINGS = mapper();

export const get = async ({ connector }, id) => connector.get(id, MAPPINGS);

export const save = async ({ connector, /* istanbul ignore next */username = 'system' }, id, input) => {
  const timestamp = now();
  return connector.update(
    {
      pk: id,
      sk: DISCRIMINATOR,
    },
    {
      discriminator: DISCRIMINATOR,
      lastModifiedBy: username,
      timestamp,
      deleted: null,
      latched: null,
      ttl: ttl(timestamp, 33),
      ...input,
    },
  );
};

export const del = async ({ connector, /* istanbul ignore next */username = 'system' }, id) => {
  const timestamp = now();
  return connector.update(
    {
      pk: id,
      sk: DISCRIMINATOR,
    },
    {
      deleted: true,
      lastModifiedBy: username,
      timestamp,
      latched: null,
      ttl: ttl(timestamp, 11),
    },
  );
};

export const toUpdateRequest = (uow) => ({
  Key: {
    pk: uow.event.thing.id,
    sk: DISCRIMINATOR,
  },
  ...updateExpression({
    ...uow.event.thing,
    discriminator: DISCRIMINATOR,
    lastModifiedBy: 'system',
    timestamp: uow.event.timestamp,
    deleted: uow.event.type === 'thing-deleted' ? true : null,
    latched: true,
    ttl: ttl(uow.event.timestamp, 33),
  }),
  ...timestampCondition(),
});

export const toEvent = (uow) => ({
  thing: MAPPINGS(uow.event.raw.new),
  raw: undefined,
});
