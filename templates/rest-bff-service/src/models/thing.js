import { updateExpression, timestampCondition } from 'aws-lambda-stream';

import {
  now, ttl, aggregateMapper, mapper,
} from '../utils';

export const DISCRIMINATOR = 'thing';

export const MAPPER = mapper();

const AGGREGATE_MAPPER = aggregateMapper({
  aggregate: DISCRIMINATOR,
  cardinality: {
  },
  mappers: {
    [DISCRIMINATOR]: MAPPER,
  },
});

export const get = async ({ connector }, id) => connector.get(id).then((data) => AGGREGATE_MAPPER(data));

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
      deleted: null,
      latched: null,
      ttl: ttl(timestamp, 33),
      ...input,
      timestamp,
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
      discriminator: DISCRIMINATOR,
      deleted: true,
      lastModifiedBy: username,
      latched: null,
      ttl: ttl(timestamp, 11),
      timestamp,
    },
  );
};

export const toUpdateRequest = (uow) => ({
  Key: {
    pk: uow.event.thing.id,
    sk: DISCRIMINATOR,
  },
  ...updateExpression({
    ...uow.event.thing, // TODO minus id, others ???
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
  thing: MAPPER(uow.event.raw.new),
  raw: undefined,
});
