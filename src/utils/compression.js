import zlib from 'zlib';
import sizeof from 'object-sizeof';

export const COMPRESSION_PREFIX = 'COMPRESSED';

export const zip = (str) => zlib.gzipSync(Buffer.from(str)).toString('base64');
export const unzip = (str) => zlib.gunzipSync(Buffer.from(str, 'base64')).toString();

// JSON.stringify replacer
export const compress = (opt = { compressionThreshold: 1024 * 10, compressionIgnore: [] }) =>
  (key, value) =>
    (key
    && /* no key is the top */ !opt.compressionIgnore?.includes(key)
    && sizeof(value) > opt.compressionThreshold
      ? `${COMPRESSION_PREFIX}${zip(JSON.stringify(value))}`
      : value);

// JSON.parse reviver
export const decompress = (key, value) =>
  (typeof value === 'string' && value.startsWith(COMPRESSION_PREFIX)
    ? JSON.parse(unzip(value.substring(COMPRESSION_PREFIX.length)))
    : value);
