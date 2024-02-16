import cloneDeepWith from 'lodash/cloneDeepWith';

export const trimAndRedact = (_uow) => { // eslint-disable-line import/prefer-default-export
  const fields = (_uow.batch || [_uow]).reduce((a, c) => {
    const eem = c.event?.eem || c.undecryptedEvent?.eem;
    const f = eem?.fields || [];
    return [...a, ...f];
  }, []);
  const cache = [];

  const cloneCustomizer = (value, key) => {
    if (fields.includes(key)) {
      return '[REDACTED]';
    } else {
      if (Buffer.isBuffer(value)) {
        return `[BUFFER: ${value.length}]`;
      }

      if (typeof value === 'object' && value !== null) {
        if (cache.includes(value)) {
          return '[CIRCULAR]';
        } else {
          cache.push(value);
        }
      }
    }
    return undefined;
  };

  const tr = (uow) => {
    const {
      pipeline, record, event, decryptResponse, undecryptedEvent, encryptResponse, ...rest // eslint-disable-line no-unused-vars
    } = uow;

    return {
      pipeline,
      record, // DO NOT redact so we can resubmit
      ...cloneDeepWith({
        event: undecryptedEvent || event,
        ...rest,
      }, cloneCustomizer),
    };
  };

  if (!_uow.batch) {
    return tr(_uow);
  } else {
    const { batch, ...rest2 } = _uow;
    return {
      batch: batch.map((u) => tr(u)),
      ...cloneDeepWith({
        ...rest2,
      }, cloneCustomizer),
    };
  }
};
