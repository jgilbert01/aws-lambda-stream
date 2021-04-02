import _ from 'highland';
import merge from 'lodash/merge';
import memoryCache from 'memory-cache';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const updateExpression = (Item) => ({
  ExpressionAttributeNames: Object.keys(Item)
    .map((attrName) => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {}),
  ExpressionAttributeValues: Object.keys(Item)
    .map((attrName) => ({ [`:${attrName}`]: Item[attrName] || null }))
    .reduce(merge, {}),
  UpdateExpression: `SET ${Object.keys(Item)
    .map((attrName) => `#${attrName} = :${attrName}`)
    .join(', ')}`,
  ReturnValues: 'ALL_NEW',
});

export const timestampCondition = (fieldName = 'timestamp') => ({
  ConditionExpression: `attribute_not_exists(#${fieldName}) OR #${fieldName} < :${fieldName}`,
});

export const update = ({
  debug = d('dynamodb'),
  tableName = process.env.ENTITY_TABLE_NAME,
  updateRequestField = 'updateRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
} = {}) => {
  const connector = new Connector({ debug, tableName });

  const invoke = (uow) => {
    const p = connector.update(uow[updateRequestField])
      .then((updateResponse) => ({ ...uow, updateResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

export const put = ({
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME,
  putRequestField = 'putRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
} = {}) => {
  const connector = new Connector({ debug, tableName });

  const invoke = (uow) => {
    const p = connector.put(uow[putRequestField])
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

export const query = (/* istanbul ignore next */{
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME,
  queryRequestField = 'queryRequest',
  queryResponseField = 'queryResponse',
  parallel = Number(process.env.QUERY_PARALLEL) || Number(process.env.PARALLEL) || 4,
} = {}) => {
  const connector = new Connector({ debug, tableName });

  const invoke = (uow) => {
    if (!uow[queryRequestField]) return _(Promise.resolve(uow));

    const req = JSON.stringify(uow[queryRequestField]);
    const cached = memoryCache.get(req);

    const p = (cached
      ? Promise.resolve(cached)
      : connector.query(uow[queryRequestField])
        .then((queryResponse) => {
          memoryCache.put(req, queryResponse);
          return queryResponse;
        })
    )
      .then((queryResponse) => ({ ...uow, [queryResponseField]: queryResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};
