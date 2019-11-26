// used after highland batch step
export const toBatchUow = (batch) => ({ batch });

// use with flatMap
export const unBatchUow = (uow) => uow.batch;
