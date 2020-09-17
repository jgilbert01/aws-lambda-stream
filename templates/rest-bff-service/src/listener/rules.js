import { materialize } from 'aws-lambda-stream';

import { toUpdateRequest as toThingUpdateRequest } from '../models/thing';

export default [
  {
    id: 'l1',
    flavor: materialize,
    eventType: /thing-(submitted|created|updated|deleted)/,
    toUpdateRequest: toThingUpdateRequest,
  },
];
