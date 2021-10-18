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

class Model {
  constructor(
    debug,
    connector,
    username = 'system',
    claims,
  ) {
    this.debug = debug;
    this.connector = connector;
    this.username = username;
    this.claims = claims;
  }

  get(id) {
    return this.connector.get(id)
      .then((data) => AGGREGATE_MAPPER(data));
  }

  save(id, input) {
    const timestamp = now();

    return this.connector.update(
      {
        pk: id,
        sk: DISCRIMINATOR,
      },
      {
        discriminator: DISCRIMINATOR,
        lastModifiedBy: this.username,
        deleted: null,
        latched: null,
        ttl: ttl(timestamp, 33),
        ...input,
        timestamp,
      },
    );
  }

  delete(id) {
    const timestamp = now();
    return this.connector.update(
      {
        pk: id,
        sk: DISCRIMINATOR,
      },
      {
        discriminator: DISCRIMINATOR,
        deleted: true,
        lastModifiedBy: this.username,
        latched: null,
        ttl: ttl(timestamp, 11),
        timestamp,
      },
    );
  }
}

export default Model;

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

export const toEvent = async (uow) => ({
  thing: await MAPPER(uow.event.raw.new),
  raw: undefined,
});
