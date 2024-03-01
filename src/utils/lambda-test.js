/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { NodeHttpHandler } from '@smithy/node-http-handler';

import { defaultDebugLogger } from './log';
import { debug as d } from './print';

export default ({
  functionName,
  endpoint = 'http://localhost:3002',
  timeout = 1000,
  debug = d('test'),

}) => {
  const lambda = new LambdaClient({
    endpoint,
    requestHandler: new NodeHttpHandler({
      requestTimeout: timeout,
      connectionTimeout: timeout,
    }),
    logger: defaultDebugLogger(debug),
  });

  return (event) => lambda
    .send(new InvokeCommand({
      FunctionName: functionName,
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify(event),
    }))
    .then((resp) => ({
      ...resp,
      Payload: JSON.parse(Buffer.from(resp.Payload)),
    }));
};
