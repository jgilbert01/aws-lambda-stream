import _ from 'highland';
import merge from 'lodash/merge';
import memoryCache from 'memory-cache';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const updateExpression = (Item) => {
  const keys = Object.keys(Item);

  const ExpressionAttributeNames = keys
    .filter((attrName) => Item[attrName] !== undefined)
    .map((attrName) => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {});

  const ExpressionAttributeValues = keys
    .filter((attrName) => Item[attrName] !== undefined && Item[attrName] !== null)
    .map((attrName) => ({ [`:${attrName}`]: Item[attrName] }))
    .reduce(merge, {});

  let UpdateExpression = `SET ${keys
    .filter((attrName) => Item[attrName] !== undefined && Item[attrName] !== null)
    .map((attrName) => `#${attrName} = :${attrName}`)
    .join(', ')}`;

  const UpdateExpressionRemove = keys
    .filter((attrName) => Item[attrName] === null)
    .map((attrName) => `#${attrName}`)
    .join(', ');

  if (UpdateExpressionRemove.length) {
    UpdateExpression = `${UpdateExpression} REMOVE ${UpdateExpressionRemove}`;
  }

  return {
    ExpressionAttributeNames,
    ExpressionAttributeValues,
    UpdateExpression,
    ReturnValues: 'ALL_NEW',
  };
};

export const timestampCondition = (fieldName = 'timestamp') => ({
  ConditionExpression: `attribute_not_exists(#${fieldName}) OR #${fieldName} < :${fieldName}`,
});

export const update = ({
  debug = d('dynamodb'),
  tableName = process.env.ENTITY_TABLE_NAME || process.env.EVENT_TABLE_NAME,
  updateRequestField = 'updateRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const invoke = (uow) => {
    if (!uow[updateRequestField]) return _(Promise.resolve(uow));

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
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  putRequestField = 'putRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const invoke = (uow) => {
    if (!uow[putRequestField]) return _(Promise.resolve(uow));

    const p = connector.put(uow[putRequestField])
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

export const batchGet = ({
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  batchGetRequestField = 'batchGetRequest',
  batchGetResponseField = 'batchGetResponse',
  parallel = Number(process.env.GET_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const invoke = (uow) => {
    if (!uow[batchGetRequestField]) return _(Promise.resolve(uow));

    const p = connector.batchGet(uow[batchGetRequestField])
      .then((batchGetResponse) => ({ ...uow, [batchGetResponseField]: batchGetResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

export const query = (/* istanbul ignore next */{
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  queryRequestField = 'queryRequest',
  queryResponseField = 'queryResponse',
  parallel = Number(process.env.QUERY_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

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
