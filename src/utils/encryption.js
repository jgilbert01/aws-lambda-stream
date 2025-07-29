import _ from 'highland';
import Promise from 'bluebird';
import omit from 'lodash/omit';
import isEmpty from 'lodash/isEmpty';
import { decryptObject, encryptObject } from 'aws-kms-ee';

import { rejectWithFault } from './faults';

import { debug as d } from './print';

// -----------------------------------
// used in listeners index
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

    const p = Promise.resolve()
      .then(() => {
        try {
          return decryptObject(omit(uow.event, eemField), { ...uow.event[eemField], AES });
        } catch (err) {
          // istanbul ignore next
          return Promise.reject(err);
        }
      })
      // .tap(debug)
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
// used in trigger index
// -----------------------------------

export const decryptChangeEvent = ({
  debug = d('enc'),
  prefilter = () => false,
  eemField = 'eem',
  AES = true,
  decryptOld = true,
  parallel = Number(process.env.ENCRYPTION_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const decrypt = (uow) => {
    if (!((uow.event.raw.new || uow.event.raw.old)[eemField]) || !prefilter(uow)) {
      return _(Promise.resolve(uow));
    }

    const p = Promise.resolve()
      .then(() => {
        try {
          return Promise.all([
            uow.event.raw.new
              ? decryptObject(omit(uow.event.raw.new, eemField), { ...uow.event.raw.new[eemField], AES })
              : Promise.resolve(undefined),
            decryptOld && uow.event.raw.old
              ? decryptObject(omit(uow.event.raw.old, eemField), { ...uow.event.raw.old[eemField], AES })
              : Promise.resolve(undefined),
          ]);
        } catch (err) {
          // istanbul ignore next
          return Promise.reject(err);
        }
      })
      // .tap(debug)
      .tapCatch(debug)
      .then((decryptResponse) => ({
        ...uow,
        decryptResponse,
        event: {
          ...uow.event,
          raw: {
            new: decryptResponse[0]?.object,
            old: decryptResponse[1]?.object,
          },
        },
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
// used in trigger pipelines prior to publish
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
    if (!eem || !uow[sourceField]) {
      return _(Promise.resolve(uow));
    }

    const eemMetadata = typeof eem === 'function' ? eem(uow[sourceField], uow) : eem;
    const p = encryptObject(uow[sourceField], {
      masterKeyAlias,
      regions,
      ...eemMetadata, // fields and overrides
      AES,
    })
      // .tap(debug)
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

// -------------------------------------
// used in listener pipelines prior to save
// -------------------------------------

export const encryptData = ({
  debug = d('enc'),
  eem,
  eemField = 'eem',
  masterKeyAlias = process.env.MASTER_KEY_ALIAS,
  regions = (process.env.KMS_REGIONS && process.env.KMS_REGIONS.split(',')),
  AES = true,
} = {}) => async (data, ctx = {}) => {
  const eemMetadata = typeof eem === 'function' ? eem(data, ctx) : eem;
  const result = await encryptObject(data, {
    masterKeyAlias,
    regions,
    ...eemMetadata, // fields and overrides
    AES,
  })
    // .tap(debug)
    .tapCatch(debug);

  return {
    ...result.encrypted,
    // storing the metadata with the data
    [eemField]: result.metadata,
  };
};

// -----------------------------------
// used in query functions
// -----------------------------------

export const decryptData = ({
  debug = d('enc'),
  eemField = 'eem',
  AES = true,
} = {}) => async (data) => {
  if (isEmpty(data)) return data;
  if (!data[eemField]) return data;

  const result = await decryptObject(omit(data, eemField), {
    ...data[eemField],
    AES,
  })
    // .tap(debug)
    .tapCatch(debug);

  return result.object;
};
