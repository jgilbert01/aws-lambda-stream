import { isServerError, isThrottlingError, isTransientError } from '@smithy/service-error-classification';
import Promise from 'bluebird';

export const defaultRetryConfig = {
  maxRetries: Number(process.env.BATCH_MAX_RETRIES) || 8,
  retryWait: Number(process.env.BATCH_RETRY_WAIT) || 100,
};

export const assertMaxRetries = (attempts, maxRetries) => {
  if (attempts.length > maxRetries) {
    throw new Error(`Failed batch requests: ${JSON.stringify(attempts[attempts.length - 1])}`);
  }
};

export const wait = (ms) => new Promise((r) => (ms ? setTimeout(r, ms) : r()));

export const getDelay = (baseMillis, attempt) => {
  if (!attempt) return 0;
  const expBackoff = 2 ** attempt;
  const finalBackoff = expBackoff + Math.round(Math.random() * baseMillis);
  return baseMillis + finalBackoff;
};

export const defaultBackoffDelay = (attempt) => getDelay(Number(process.env.BASE_BACKOFF_MILLIS) || 200, attempt);

export const retryable = (err, opt) =>
  (isThrottlingError(err) || isTransientError(err) || isServerError(err))
  && process.env.STREAM_RETRY_ENABLED === 'true';
