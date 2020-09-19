import { debug as d } from 'debug';

import Connector from '../connectors/dynamodb';
import { getThing, saveThing, deleteThing } from './routes/thing';

const api = require('lambda-api')({ version: 'v1.0', base: 'v1' });

api.use((req, res, next) => {
  res.cors();
  next();
});

api.get('/things/:id', getThing);
api.put('/things/:id', saveThing);
api.delete('/things/:id', deleteThing);

export const handle = async (event, context) => { // eslint-disable-line import/prefer-default-export
  const debug = d(`handler${event.path.split('/').join(':')}`);
  debug('event: %j', event);
  // debug(`ctx: %j`, context);
  // debug(`env: %j`, process.env);

  api.app({
    debug,
    connector: new Connector(
      debug,
      process.env.ENTITY_TABLE_NAME,
    ),
    username: (event.requestContext.authorizer.claims
      && event.requestContext.authorizer.claims['cognito:username'])
      || event.requestContext.authorizer.principalId,
  });

  return api.run(event, context);
};
