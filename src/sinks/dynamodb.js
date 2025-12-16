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

/**
 * A more flexible (but verbose) variant of updateExpression.
 * Requires providing an Item in the form of 'item fragments' which are
 * functions supporting the varying DDB operations. This function makes no assumptions
 * about your incoming values (except for removing undefined). If you need to remove
 * a value for example, you need to specify remove.
 */
export const updateExpressionFromFragments = (ItemFragments) => {
  const exprAttributes = Object.entries(ItemFragments)
    .filter(([, fragmentGenerator]) => fragmentGenerator !== undefined)
    .reduce((acc, [key, fragmentGenerator]) => {
      const {
        nameFragment,
        valueFragment,
        setFragment,
        removeFragment,
        addFragment,
        deleteFragment,
      } = fragmentGenerator(key);

      return {
        ExpressionAttributeNames: {
          ...acc.ExpressionAttributeNames,
          ...nameFragment,
        },
        ExpressionAttributeValues: {
          ...acc.ExpressionAttributeValues,
          ...valueFragment,
        },
        setFragments: setFragment ? [...acc.setFragments, setFragment] : acc.setFragments,
        addFragments: addFragment ? [...acc.addFragments, addFragment] : acc.addFragments,
        deleteFragments: deleteFragment ? [...acc.deleteFragments, deleteFragment] : acc.deleteFragments,
        removeFragments: removeFragment ? [...acc.removeFragments, removeFragment] : acc.removeFragments,
      };
    }, {
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      setFragments: [],
      addFragments: [],
      deleteFragments: [],
      removeFragments: [],
    });

  // Construct UpdateExpression
  const updateExpressionParts = [];
  if (exprAttributes.setFragments.length) updateExpressionParts.push(`SET ${exprAttributes.setFragments.join(', ')}`);
  if (exprAttributes.removeFragments.length) updateExpressionParts.push(`REMOVE ${exprAttributes.removeFragments.join(', ')}`);
  if (exprAttributes.addFragments.length) updateExpressionParts.push(`ADD ${exprAttributes.addFragments.join(', ')}`);
  if (exprAttributes.deleteFragments.length) updateExpressionParts.push(`DELETE ${exprAttributes.deleteFragments.join(', ')}`);
  const UpdateExpression = updateExpressionParts.join(' ');

  return {
    ExpressionAttributeNames: exprAttributes.ExpressionAttributeNames,
    ExpressionAttributeValues: exprAttributes.ExpressionAttributeValues,
    UpdateExpression,
    ReturnValues: 'ALL_NEW',
  };
};

/**
 * Fragment generators.
 * Only set and setNested support operand resolvers.
 */
export const setValue = (value, { atIndex } = {}) => (attributeKey) => {
  const isResolver = value.__isResolver__;
  const resolvedValue = isResolver ? value.resolvedValue : value;
  const setFragmentValue = isResolver ? value.top(attributeKey) : `:${attributeKey}`;

  return {
    nameFragment: {
      [`#${attributeKey}`]: attributeKey,
    },
    valueFragment: {
      [`:${attributeKey}`]: resolvedValue,
    },
    setFragment: `#${attributeKey}${atIndex !== undefined ? `[${atIndex}]` : ''} = ${setFragmentValue}`,
  };
};

export const setNestedValue = (value, { atIndex } = {}) => (attributeKey) => {
  const isResolver = value.__isResolver__;
  const resolvedValue = isResolver ? value.resolvedValue : value;
  const setFragmentValue = isResolver ? value.nested(attributeKey) : `:${attributeKey}`;

  return {
    nameFragment: Object.fromEntries(attributeKey.split('.').map((kp) => [`#${kp}`, kp])),
    valueFragment: {
      [`:${attributeKey}`]: resolvedValue,
    },
    setFragment: `${attributeKey.split('.').map((kp) => `#${kp}`).join('.')}${atIndex !== undefined ? `[${atIndex}]` : ''} = ${setFragmentValue}`,
  };
};

export const removeValue = ({ atIndex } = {}) => (attributeKey) => ({
  nameFragment: { [`#${attributeKey}`]: attributeKey },
  removeFragment: `#${attributeKey}${atIndex !== undefined ? `[${atIndex}]` : ''}`,
});

export const removeNestedValue = ({ atIndex } = {}) => (attributeKey) => ({
  nameFragment: Object.fromEntries(attributeKey.split('.').map((ak) => [`#${ak}`, ak])),
  removeFragment: `${attributeKey.split('.').map((ak) => `#${ak}`).join('.')}${atIndex !== undefined ? `[${atIndex}]` : ''}`,
});

export const addToSet = (value) => (attributeKey) => ({
  nameFragment: { [`#${attributeKey}`]: attributeKey },
  valueFragment: { [`:${attributeKey}__add`]: value },
  addFragment: `#${attributeKey} :${attributeKey}__add`,
});

export const deleteFromSet = (value) => (attributeKey) => ({
  nameFragment: { [`#${attributeKey}`]: attributeKey },
  valueFragment: { [`:${attributeKey}__delete`]: value },
  deleteFragment: `#${attributeKey} :${attributeKey}__delete`,
});

export const addAndDeleteFromSet = (addValues, deleteValues) => (attributeKey) => {
  const { addFragment, valueFragment: vfAdd } = addToSet(addValues)(attributeKey);
  const { deleteFragment, valueFragment: vfDelete, nameFragment } = deleteFromSet(deleteValues)(attributeKey);
  return {
    nameFragment,
    addFragment,
    deleteFragment,
    valueFragment: {
      ...vfAdd,
      ...vfDelete,
    },
  };
};

/* Operand resolvers */
export const ifNotExists = (value) => ({
  __isResolver__: true,
  resolvedValue: value,
  top: (attributeKey) => `if_not_exists(#${attributeKey}, :${attributeKey})`,
  nested: (attributeKey) => `if_not_exists(${attributeKey.split('.').map((ak) => `#${ak}`).join('.')}), :${attributeKey}`,
});

export const incrementBy = (value) => ({
  __isResolver__: true,
  resolvedValue: value,
  top: (attributeKey) => `#${attributeKey} + :${attributeKey}`,
  nested: (attributeKey) => `${attributeKey.split('.').map((ak) => `#${ak}`).join('.')} + :${attributeKey}`,
});

export const decrementBy = (value) => ({
  __isResolver__: true,
  resolvedValue: value,
  top: (attributeKey) => `#${attributeKey} - :${attributeKey}`,
  nested: (attributeKey) => `${attributeKey.split('.').map((ak) => `#${ak}`).join('.')} - :${attributeKey}`,
});

export const append = (value) => ({
  __isResolver__: true,
  resolvedValue: [].concat(value),
  top: (attributeKey) => `list_append(#${attributeKey}, :${attributeKey}`,
  nested: (attributeKey) => `list_append(${attributeKey.split('.').map((ak) => `#${ak}`).join('.')}, :${attributeKey})`,
});

export const prepend = (value) => ({
  __isResolver__: true,
  resolvedValue: [].concat(value),
  top: (attributeKey) => `list_append(:${attributeKey}, #${attributeKey})`,
  nested: (attributeKey) => `list_append(:${attributeKey}, ${attributeKey.split('.').map((ak) => `#${ak}`).join('.')})`,
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
