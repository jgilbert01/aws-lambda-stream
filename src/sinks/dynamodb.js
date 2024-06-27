import _ from 'highland';
import merge from 'lodash/merge';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

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

export const pkCondition = (fieldName = 'pk') => ({
  ConditionExpression: `attribute_not_exists(${fieldName})`,
});

export const updateDynamoDB = ({
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.ENTITY_TABLE_NAME || process.env.EVENT_TABLE_NAME,
  updateRequestField = 'updateRequest',
  updateResponseField = 'updateResponse',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  removeUndefinedValues = true,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, removeUndefinedValues, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[updateRequestField]) return _(Promise.resolve(uow));

    const p = connector.update(uow[updateRequestField], uow)
      .then((updateResponse) => ({ ...uow, [updateResponseField]: updateResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, 'save') || p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};

export const putDynamoDB = ({
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  putRequestField = 'putRequest',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[putRequestField]) return _(Promise.resolve(uow));

    const p = connector.put(uow[putRequestField], uow)
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, 'save') || p); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};
