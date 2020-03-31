import _ from 'highland';
import merge from 'lodash/merge';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from './faults';
import { debug as d } from './print';

export const updateExpression = (Item) => ({
  ExpressionAttributeNames: Object.keys(Item)
    .map((attrName) => ({ [`#${attrName}`]: attrName }))
    .reduce(merge, {}),
  ExpressionAttributeValues: Object.keys(Item)
    .map((attrName) => ({ [`:${attrName}`]: Item[attrName] }))
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
