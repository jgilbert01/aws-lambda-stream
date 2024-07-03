/* eslint-disable import/prefer-default-export */
export const capture = (client, command, connector, opt, ctx) => {
  // console.log('capture: ', connector, opt, ctx);
  // TODO addMiddleware(client, command, connector, opt, ctx);
  if (opt.xrayEnabled) {
    // Wraps client with xray middlware.
    require('./xray').captureSdkClientTraces(client);
  }
  return client;
};
