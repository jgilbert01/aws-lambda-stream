import _omit from 'lodash/omit';

export const debug = require('debug')('handler');

export const now = () => Date.now();

export const ttl = (start, days) => Math.floor(start / 1000) + (60 * 60 * 24 * days);

export const DEFAULT_OMIT_FIELDS = [
  'pk',
  'sk',
  'data',
  'discriminator',
  'ttl',
  'latched',
  'deleted',
  'aws:rep:updateregion',
  'aws:rep:updatetime',
  'aws:rep:deleting',
];

export const DEFAULT_RENAME = { pk: 'id' };

export const mapper = ({
  defaults = {},
  rename = DEFAULT_RENAME,
  omit = DEFAULT_OMIT_FIELDS,
  transform = {},
} = {}) => (o) => {
  const transformed = {
    ...o,
    ...Object.keys(transform).reduce((a, k) => {
      if (o[k]) a[k] = transform[k](o[k]);
      return a;
    }, {}),
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

export const aggregateMapper = ({
  aggregate,
  cardinality,
  mappers,
  delimiter = '|',
}) => (items) => items.reduce((a, c) => {
  const mappings = mappers[c.discriminator] || /* istanbul ignore next */ ((o) => o);
  const mapped = mappings(c);

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
