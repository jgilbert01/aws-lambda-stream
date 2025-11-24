import _ from 'highland';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';
import { ratelimit } from '../utils/ratelimit';

export const updateExpression = (Item) => {
  const exprAttributes = Object.entries(Item)
    .filter(([key, value]) => value !== undefined)
    .reduce((acc, [key, value]) => {
      // If this attribute ends with '_delete'...assume we're deleting values from a set.
      const isDeleteSet = key.endsWith('_delete');
      const baseKey = isDeleteSet ? key.replace(/_delete$/, '') : key;
      acc.ExpressionAttributeNames[`#${baseKey}`] = baseKey;

      if (value === null) {
        acc.removeClauses.push(`#${baseKey}`);
        return acc;
      }

      if (isDeleteSet) {
        let setValue = value;
        if (!(setValue instanceof Set)) {
          setValue = new Set([setValue]);
        }
        acc.ExpressionAttributeValues[`:${key}`] = setValue;
        acc.deleteClauses.push(`#${baseKey} :${key}`);
        return acc;
      }

      if (value instanceof Set) {
        acc.ExpressionAttributeValues[`:${key}`] = value;
        acc.addClauses.push(`#${key} :${key}`);
        return acc;
      }

      acc.ExpressionAttributeValues[`:${key}`] = value;
      acc.setClauses.push(`#${key} = :${key}`);
      return acc;
    }, {
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      setClauses: [],
      addClauses: [],
      deleteClauses: [],
      removeClauses: [],
    });

  // Construct UpdateExpression
  const updateExpressionParts = [];
  if (exprAttributes.setClauses.length) updateExpressionParts.push(`SET ${exprAttributes.setClauses.join(', ')}`);
  if (exprAttributes.removeClauses.length) updateExpressionParts.push(`REMOVE ${exprAttributes.removeClauses.join(', ')}`);
  if (exprAttributes.addClauses.length) updateExpressionParts.push(`ADD ${exprAttributes.addClauses.join(', ')}`);
  if (exprAttributes.deleteClauses.length) updateExpressionParts.push(`DELETE ${exprAttributes.deleteClauses.join(', ')}`);
  const UpdateExpression = updateExpressionParts.join(' ');

  return {
    ExpressionAttributeNames: exprAttributes.ExpressionAttributeNames,
    ExpressionAttributeValues: exprAttributes.ExpressionAttributeValues,
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
  throwConditionFailure = false,
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, removeUndefinedValues, throwConditionFailure, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[updateRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.update(uow[updateRequestField], uow)
      .then((updateResponse) => ({ ...uow, [updateResponseField]: updateResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
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
  step = 'save',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[putRequestField]) return _(Promise.resolve(uow));

    const p = () => connector.put(uow[putRequestField], uow)
      .then((putResponse) => ({ ...uow, putResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};
