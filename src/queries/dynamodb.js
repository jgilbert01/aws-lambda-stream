import _ from 'highland';
import memoryCache from 'memory-cache';

import Connector from '../connectors/dynamodb';

import { rejectWithFault } from '../utils/faults';
import { debug as d } from '../utils/print';

export const batchGetDynamoDB = ({
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  batchGetRequestField = 'batchGetRequest',
  batchGetResponseField = 'batchGetResponse',
  parallel = Number(process.env.GET_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  decrypt = async (data) => data,
  step = 'get',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[batchGetRequestField]) return _(Promise.resolve(uow));

    const req = JSON.stringify(uow[batchGetRequestField]);
    const cached = memoryCache.get(req);

    const p = () => (cached
      ? /* istanbul ignore next */ Promise.resolve(cached)
      : connector.batchGet(uow[batchGetRequestField], uow)
        .then(async (batchGetResponse) => ({
          ...batchGetResponse,
          Responses: await Object.keys(batchGetResponse.Responses).reduce(async (a, c) => {
            a = await a;
            return {
              ...a,
              [c]: await Promise.all(batchGetResponse.Responses[c]?.map(decrypt)),
            };
          }, {}),
        }))
        .then((batchGetResponse) => {
          memoryCache.put(req, batchGetResponse);
          return batchGetResponse;
        })
    )
      .then((batchGetResponse) => ({ ...uow, [batchGetResponseField]: batchGetResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

export const queryAllDynamoDB = (/* istanbul ignore next */{
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME,
  queryRequestField = 'queryRequest',
  queryResponseField = 'queryResponse',
  parallel = Number(process.env.QUERY_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  decrypt = async (data) => data,
  step = 'query',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[queryRequestField]) return _(Promise.resolve(uow));

    const req = JSON.stringify(uow[queryRequestField]);
    const cached = memoryCache.get(req);

    const p = () => (cached
      ? Promise.resolve(cached)
      : connector.query(uow[queryRequestField], uow)
        .then((queryResponse) => Promise.all(queryResponse.map(decrypt)))
        .then((queryResponse) => {
          memoryCache.put(req, queryResponse);
          return queryResponse;
        })
    )
      .then((queryResponse) => ({ ...uow, [queryResponseField]: queryResponse }))
      .catch(rejectWithFault(uow));

    return _(uow.metrics?.w(p, step) || p()); // wrap promise in a stream
  };

  return (s) => s
    .map(invoke)
    .parallel(parallel);
};

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
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME || process.env.TABLE_NAME,
  scanRequestField = 'scanRequest',
  scanResponseField = 'scanResponse',
  parallel = Number(process.env.SCAN_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  decrypt = async (data) => data,
  step = 'scan',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const scan = (uow) => {
    if (!uow[scanRequestField]) return _(Promise.resolve(uow));

    let cursor = uow[scanRequestField].ExclusiveStartKey;
    let itemsCount = 0;

    return _((push, next) => {
      const params = {
        ...uow[scanRequestField],
        ExclusiveStartKey: cursor,
      };

      const p = () => connector.scan(params, uow);
      (uow.metrics?.w(p, step) || p())
        .then(async ({ LastEvaluatedKey, Items, ...rest }) => ({ LastEvaluatedKey, Items: await Promise.all(Items.map(decrypt)), ...rest }))
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
                LastEvaluatedKey,
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

export const querySplitDynamoDB = ({
  id: pipelineId,
  debug = d('dynamodb'),
  tableName = process.env.EVENT_TABLE_NAME || process.env.ENTITY_TABLE_NAME || process.env.TABLE_NAME,
  querySplitRequestField = 'querySplitRequest',
  querySplitResponseField = 'querySplitResponse',
  parallel = Number(process.env.SCAN_PARALLEL) || Number(process.env.PARALLEL) || 4,
  timeout = Number(process.env.DYNAMODB_TIMEOUT) || Number(process.env.TIMEOUT) || 1000,
  decrypt = async (data) => data,
  step = 'query',
  ...opt
} = {}) => {
  const connector = new Connector({
    pipelineId, debug, tableName, timeout, ...opt,
  });

  const invoke = (uow) => {
    if (!uow[querySplitRequestField]) return _(Promise.resolve(uow));

    let cursor = uow[querySplitRequestField].ExclusiveStartKey;
    let itemsCount = 0;

    return _((push, next) => {
      const params = {
        ...uow[querySplitRequestField],
        ExclusiveStartKey: cursor,
      };

      const p = () => connector.queryPage(params, uow);
      (uow.metrics?.w(p, step) || p())
        .then(async ({ LastEvaluatedKey, Items, ...rest }) => ({ LastEvaluatedKey, Items: await Promise.all(Items.map(decrypt)), ...rest }))
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
                LastEvaluatedKey,
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
