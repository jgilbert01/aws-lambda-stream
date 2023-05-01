export const ratelimit = (rule) => { // eslint-disable-line import/prefer-default-export
  if (!rule.rate) return (s) => s;

  return (s) => s
    .ratelimit(rule.rate.num, rule.rate.ms);
};
