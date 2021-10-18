import { debug as d } from 'debug';

import Connector from '../connectors/dynamodb';
import Model from '../models/thing';
import { getThing, saveThing, deleteThing } from './routes/thing';
import { getUsername, getClaims/* , forRole */ } from '../utils';

const api = require('lambda-api')();

api.use((req, res, next) => {
  res.cors();
  next();
});

api.get('/things/:id', getThing);
api.put('/things/:id', /* forRole('power'), */ saveThing);
api.delete('/things/:id', /* forRole('admin'), */ deleteThing);

export const handle = async (event, context) => { // eslint-disable-line import/prefer-default-export
  const debug = d(`handler${event.path.split('/').join(':')}`);
  debug('event: %j', event);
  // debug(`ctx: %j`, context);
  // debug(`env: %j`, process.env);

  const claims = getClaims(event.requestContext);
  const username = getUsername(event.requestContext);

  api.app({
    debug,
    models: {
      thing: new Model(
        debug,
        new Connector(
          debug,
          process.env.ENTITY_TABLE_NAME,
        ),
        username,
        claims,
      ),
    },
  });

  return api.run(event, context);
};
