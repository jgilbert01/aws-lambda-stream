import Connector from '../connectors/secretsmgr';

let connector;

export const getSecrets = async (options) => { // eslint-disable-line import/prefer-default-export
  /* istanbul ignore else */
  if (!connector) {
    connector = new Connector({
      secretId: `${process.env.PROJECT}/${process.env.STAGE}`,
      ...options,
    });
  }

  const secrets = await connector.get();

  return {
    ...options,
    ...secrets,
  };
};
