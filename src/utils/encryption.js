import _ from 'highland';
import omit from 'lodash/omit';
import { decryptObject, encryptObject } from 'aws-kms-ee';

import { rejectWithFault } from './faults';

import { debug as d } from './print';

// -----------------------------------
// used in listeners
// -----------------------------------

export const decryptEvent = ({
  debug = d('enc'),
  prefilter = () => false,
  eemField = 'eem',
  AES = true,
  parallel = Number(process.env.ENCRYPTION_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const decrypt = (uow) => {
    if (!uow.event[eemField] || !prefilter(uow)) {
      return _(Promise.resolve(uow));
    }

    const p = decryptObject(omit(uow.event, eemField), { ...uow.event[eemField], AES })
      .tap(debug)
      .tapCatch(debug)
      .then((decryptResponse) => ({
        ...uow,
        decryptResponse,
        event: decryptResponse.object,
        undecryptedEvent: uow.event,
      }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(decrypt)
    .parallel(parallel);
};

// -----------------------------------
// used in triggers prior to publish
// -----------------------------------

export const encryptEvent = ({
  debug = d('enc'),
  eemField = 'eem',
  sourceField = 'event',
  targetField = 'event',
  eem,
  masterKeyAlias = process.env.MASTER_KEY_ALIAS,
  regions = (process.env.KMS_REGIONS && process.env.KMS_REGIONS.split(',')),
  AES = true,
  parallel = Number(process.env.ENCRYPTION_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const encrypt = (uow) => {
    if (!eem) {
      return _(Promise.resolve(uow));
    }

    const p = encryptObject(uow[sourceField], {
      masterKeyAlias,
      regions,
      ...eem, // fields and overrides
      AES,
    })
      .tap(debug)
      .tapCatch(debug)
      .then((encryptResponse) => ({
        ...uow,
        encryptResponse,
        [targetField]: {
          ...encryptResponse.encrypted,
          [eemField]: encryptResponse.metadata,
        },
      }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(encrypt)
    .parallel(parallel);
};
