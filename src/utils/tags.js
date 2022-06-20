import { skipTag } from '../filters';

export const adornStandardTags = (eventField) => (uow) => ({
  ...uow,
  [eventField]: {
    ...uow[eventField],
    tags: {
      ...envTags(uow.pipeline),
      ...skipTag(),
      ...uow[eventField].tags,
    },
  },
});

export const envTags = (pipeline) => ({
  account: process.env.ACCOUNT_NAME || 'undefined',
  region: process.env.AWS_REGION || /* istanbul ignore next */ 'undefined',
  stage: process.env.STAGE || process.env.SERVERLESS_STAGE || 'undefined',
  source: process.env.SERVICE || process.env.PROJECT || process.env.SERVERLESS_PROJECT || 'undefined',
  functionname: process.env.AWS_LAMBDA_FUNCTION_NAME || 'undefined',
  pipeline: pipeline || 'undefined',
});
