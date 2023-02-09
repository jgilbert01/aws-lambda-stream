import _ from 'highland';

// used after highland batch step
export const toBatchUow = (batch) => ({ batch });

// use with flatMap
export const unBatchUow = (uow) => {
  const { batch, ...outerUowMinusBatch } = uow;
  return batch.map((innerUowFromBatch) => ({
    ...innerUowFromBatch,
    ...outerUowMinusBatch,
  }));
};

export const group = (rule) => {
  /* istanbul ignore if */
  if (!rule.group) return (s) => s;

  return (s) => s
    .group((uow) => uow.event.partitionKey)
    .flatMap(toGroupUows);
};

// use with flatMap after highland group step
export const toGroupUows = (groups) => _(Object.keys(groups).map((key) => ({ batch: groups[key] })));

export const batchWithSize = (opt) => {
  let batched = [];
  let sizes = [];

  return (err, x, push, next) => {
    /* istanbul ignore if */
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      if (batched.length > 0) {
        logMetrics(batched, sizes, opt);
        push(null, batched);
      }

      push(null, _.nil);
    } else {
      const size = Buffer.byteLength(JSON.stringify(x[opt.requestEntryField]));
      const totalSize = sizes.reduce((a, c) => a + c, size);

      if (totalSize <= opt.maxRequestSize && batched.length + 1 <= opt.batchSize) {
        batched.push(x);
        sizes.push(size);
      } else {
        logMetrics(batched, sizes, opt);
        push(null, batched);
        batched = [x];
        sizes = [size];
      }

      next();
    }
  };
};

const logMetrics = (batch, sizes, opt) => {
  if (opt.metricsEnabled) {
    opt.debug('%j', {
      metrics: {
        [opt.requestField]: {
          count: sizes.length,
          ...sizes.reduce((a, size, i) => ({
            ...a,
            average: (a.sum + size) / (i + 1),
            min: a.min < size ? a.min : size,
            max: a.max > size ? a.max : size,
            sum: a.sum + size,
            types: {
              ...a.types,
              [batch[i][opt.requestEntryField].DetailType]: [...(a.types[batch[i][opt.requestEntryField].DetailType] || []), size],
            },
          }), {
            average: 0,
            min: undefined,
            max: undefined,
            sum: 0,
            types: {},
          }),
        },
      },
    });
  }
};
