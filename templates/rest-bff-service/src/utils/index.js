import _omit from 'lodash/omit';
import get from 'lodash/get';

export const debug = require('debug')('handler');

export const now = () => Date.now();

export const ttl = (start, days) => Math.floor(start / 1000) + (60 * 60 * 24 * days);

export const getClaims = (requestContext) => ({
  ...(requestContext.authorizer?.claims || requestContext.authorizer || {}),
});

export const getUsername = (requestContext) => get(requestContext, 'authorizer.claims.cognito:username', requestContext.authorizer?.principalId || '');

export const getUserGroups = (requestContext) => get(requestContext, 'authorizer.claims.cognito:groups', '').split(',');

export const forRole = (role) => (req, res, next) => { // eslint-disable-line consistent-return
  const groups = getUserGroups(req.requestContext);
  if (groups.includes(role)) {
    return next();
  } else {
    res.error(401, 'Not Authorized');
  }
};

export const sortKeyTransform = (v) => v.split('|')[1];

export const deletedFilter = (i) => !i.deleted;

export const DEFAULT_OMIT_FIELDS = [
  'pk',
  'sk',
  'data',
  'data2',
  'data3',
  'data4',
  'discriminator',
  'ttl',
  'latched',
  'deleted',
  'aws:rep:updateregion',
  'aws:rep:updatetime',
  'aws:rep:deleting',
  'eem',
];

export const DEFAULT_RENAME = { pk: 'id' };

export const mapper = ({
  defaults = {},
  rename = DEFAULT_RENAME,
  omit = DEFAULT_OMIT_FIELDS,
  transform = {},
} = {}) => async (o, ctx = {}) => {
  const transformed = {
    ...o,
    ...(await Object.keys(transform).reduce(async (a, k) => {
      a = await a;
      if (o[k]) a[k] = await transform[k](o[k], ctx);
      return a;
    }, {})),
  };

  const renamed = {
    ...o,
    ...Object.keys(rename).reduce((a, k) => {
      if (transformed[k]) a[rename[k]] = transformed[k];
      return a;
    }, {}),
  };

  return ({
    ...defaults,
    ..._omit(renamed, omit),
  });
};

// https://advancedweb.hu/how-to-use-async-functions-with-array-reduce-in-javascript/

export const aggregateMapper = ({
  aggregate,
  cardinality,
  mappers,
  delimiter = '|',
}) => async (items, ctx = {}) => items
  .filter(deletedFilter)
  .reduce(async (a, c) => {
    a = await a;
    const mappings = mappers[c.discriminator] || /* istanbul ignore next */ (async (o) => o);
    const mapped = await mappings(c, ctx);

    if (c.discriminator === aggregate) {
      return {
        ...mapped,
        ...a,
      };
    } else {
      const role = c.sk.split(delimiter)[0];
      if (!a[role]) {
        if (cardinality[role] > 1) {
          a[role] = [mapped];
        } else {
          a[role] = mapped;
        }
      } else {
        a[role].push(mapped);
      }

      return a;
    }
  }, {});
