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
