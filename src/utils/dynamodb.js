import _ from 'highland';
import merge from 'lodash/merge';
import memoryCache from 'memory-cache';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from './faults';
import { debug as d } from './print';
import { ratelimit } from './ratelimit';

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
  debug = d('dynamodb'),
  tableName = process.env.ENTITY_TABLE_NAME || process.env.EVENT_TABLE_NAME,
  updateRequestField = 'updateRequest',
  updateResponseField = 'updateResponse',
  parallel = Number(process.env.UPDATE_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  ...opt
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const invoke = (uow) => {
    if (!uow[updateRequestField]) return _(Promise.resolve(uow));

    const p = connector.update(uow[updateRequestField])
      .then((updateResponse) => ({ ...uow, [updateResponseField]: updateResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .through(ratelimit(opt))
    .map(invoke)
    .parallel(parallel);
};

export const update = updateDynamoDB; // deprecated export

export const putDynamoDB = ({
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

export const put = putDynamoDB; // deprecated export

export const batchGetDynamoDB = ({
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

export const batchGet = batchGetDynamoDB; // deprecated export

export const queryAllDynamoDB = (/* istanbul ignore next */{
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

export const query = queryAllDynamoDB; // deprecated export

export const queryDynamoDB = queryAllDynamoDB; // deprecated export

export const toPkQueryRequest = (uow, rule) => ({
  KeyConditionExpression: '#pk = :pk',
  ExpressionAttributeNames: {
    '#pk': rule.pkFn || 'pk',
  },
  ExpressionAttributeValues: {
    ':pk': uow.event.partitionKey,
  },
  ConsistentRead: true,
});

export const toIndexQueryRequest = (uow, rule) => ({
  IndexName: rule.indexNm,
  KeyConditionExpression: '#pk = :pk',
  ExpressionAttributeNames: {
    '#pk': rule.indexFn,
  },
  ExpressionAttributeValues: {
    ':pk': uow.event.partitionKey,
  },
  ConsistentRead: false,
});

export const toGetRequest = (uow, rule) => {
  const data = uow.event.raw.new || /* istanbul ignore next */ uow.event.raw.old;

  const Keys = rule.fks
    .reduce((a, fk) => {
      const value = data[fk];
      /* istanbul ignore else */
      if (value) {
        const [discriminator, pk] = value.split('|');
        return [...a, {
          pk,
          sk: discriminator,
        }];
      } else {
        return a;
      }
    }, []);

  return {
    RequestItems: {
      [rule.tableName]: {
        Keys,
      },
    },
  };
};

export const scanSplitDynamoDB = ({
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME || process.env.TABLE_NAME,
  scanRequestField = 'scanRequest',
  scanResponseField = 'scanResponse',
  parallel = Number(process.env.SCAN_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,

} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const scan = (uow) => {
    if (!uow[scanRequestField]) return _(Promise.resolve(uow));

    let cursor;

    return _((push, next) => {
      const params = {
        ...uow[scanRequestField],
        ExclusiveStartKey: cursor,
      };
      let itemsCount = 0;

      connector.scan(params)
        .then((data) => {
          const { LastEvaluatedKey, Items, ...rest } = data;
          itemsCount += Items.length;

          if (LastEvaluatedKey && (!params.Limit || (params.Limit && itemsCount < params.Limit))) {
            cursor = LastEvaluatedKey;
          } else {
            cursor = undefined;
          }

          Items.forEach((Item) => {
            push(null, {
              ...uow,
              [scanRequestField]: params,
              [scanResponseField]: {
                ...rest,
                Item,
              },
            });
          });
        })
        .catch(/* istanbul ignore next */(err) => {
          err.uow = uow;
          push(err, null);
        })
        .finally(() => {
          if (cursor) {
            next();
          } else {
            push(null, _.nil);
          }
        });
    });
  };

  return (s) => s
    .map(scan)
    .parallel(parallel);
};

export const scanDynamoDB = scanSplitDynamoDB; // deprecated export

export const querySplitDynamoDB = ({
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME || process.env.TABLE_NAME,
  querySplitRequestField = 'querySplitRequest',
  querySplitResponseField = 'querySplitResponse',
  parallel = Number(process.env.SCAN_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
} = {}) => {
  const connector = new Connector({ debug, tableName, timeout });

  const invoke = (uow) => {
    if (!uow[querySplitRequestField]) return _(Promise.resolve(uow));

    let cursor = uow[querySplitRequestField].ExclusiveStartKey;

    return _((push, next) => {
      const params = {
        ...uow[querySplitRequestField],
        ExclusiveStartKey: cursor,
      };
      let itemsCount = 0;

      connector.queryPage(params)
        .then((data) => {
          const { LastEvaluatedKey, Items, ...rest } = data;
          itemsCount += Items.length;

          if (LastEvaluatedKey && (!params.Limit || (params.Limit && itemsCount < params.Limit))) {
            cursor = LastEvaluatedKey;
          } else {
            cursor = undefined;
          }

          Items.forEach((Item) => {
            push(null, {
              ...uow,
              [querySplitRequestField]: params,
              [querySplitResponseField]: {
                ...rest,
                Item,
              },
            });
          });
        })
        .catch(/* istanbul ignore next */(err) => {
          err.uow = uow;
          push(err, null);
        })
        .finally(() => {
          if (cursor) {
            next();
          } else {
            push(null, _.nil);
          }
        });
    });
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};
