//-----------------------------
// Integration Testing Support
//-----------------------------
// when recording integration tests, events will be generated to the stream
// and we want to ignore these test events downstream

// set the NODE_ENV variable to 'test' in the package.json
// then the framework will set the skip tag
export const skipTag = () => ({
  skip: process.env.NODE_ENV === 'test' ? true : undefined,
});

// use this filter in your pipelines to ignore these test events
export const outSkip = (uow) => {
  const keep = (!uow.event.tags || !uow.event.tags.skip);
  const isTestMode = process.env.NODE_ENV === 'test';
  if (!keep && isTestMode) {
    console.log('Ignoring event because of skip flag. Ensure this is intentional.');
  }
  return keep;
};
