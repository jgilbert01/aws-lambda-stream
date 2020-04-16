export const now = () => Date.now(); // eslint-disable-line import/prefer-default-export

export const ttl = (start, days) => (start / 1000) + (60 * 60 * 24 * days);
