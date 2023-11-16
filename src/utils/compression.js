import zlib from 'zlib';
import sizeof from 'object-sizeof';
import get from 'lodash/get';
import set from 'lodash/set';
import cloneDeep from 'lodash/cloneDeep';

export const COMPRESSION_PREFIX = 'COMPRESSED';

export const zip = (str) => zlib.gzipSync(Buffer.from(str)).toString('base64');
export const unzip = (str) => zlib.gunzipSync(Buffer.from(str, 'base64')).toString();

// JSON.stringify replacer
export const compress = (opt = { compressionThreshold: 1024 * 10, compressionHints: undefined }) => {
  if (opt.compressionHints) {
    return (key, value) => {
      if (key) return value; // dont alter after compressing

      const compressed = cloneDeep(value);
      opt.compressionHints.sort().forEach((h) => {
        const valueToCompress = get(compressed, h);
        if (valueToCompress !== undefined) {
          set(compressed, h, `${COMPRESSION_PREFIX}${zip(JSON.stringify(valueToCompress))}`);
        }
      });
      return compressed;
    };
  } else {
    return (key, value) =>
      (key /* no key is the top */ && sizeof(value) > opt.compressionThreshold
        ? `${COMPRESSION_PREFIX}${zip(JSON.stringify(value))}`
        : value);
  }
};


// JSON.parse reviver
export const decompress = (key, value) =>
  (typeof value === 'string' && value.startsWith(COMPRESSION_PREFIX)
    ? JSON.parse(unzip(value.substring(COMPRESSION_PREFIX.length)))
    : value);
