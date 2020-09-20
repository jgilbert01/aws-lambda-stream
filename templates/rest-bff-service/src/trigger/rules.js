import { crud } from 'aws-lambda-stream';

import { toEvent as toThingEvent } from '../models/thing';

export default [
  {
    id: 't1',
    flavor: crud,
    eventType: /thing-(created|updated|deleted)/,
    toEvent: toThingEvent,
  },
];
