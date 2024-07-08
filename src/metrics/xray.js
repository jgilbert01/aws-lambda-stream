import AWSXray from 'aws-xray-sdk-core';
import debug from 'debug';

const log = debug('xray');
const INPUT_CAPTURE_MIDDLEWARE_NAME = 'XraySDKClientInputCapture';

log('Initializing xray trace capture.');
AWSXray.capturePromise();
AWSXray.captureHTTPsGlobal(require('https'));

let pipelineSegments = {};

/**
 * Captures and SDK client via xray segment. Injects this sdk client with
 * additional middleware that appends the command input as metadata
 * on the subsegment.
 */
export const captureSdkClientTraces = (sdkClient) => {
  const capturedClient = AWSXray.captureAWSv3Client(sdkClient);
  applyInputCaptureMiddleware(capturedClient);
  return capturedClient;
};

const inputCaptureMiddleware = (next, context) => async (args) => {
  const segment = AWSXray.getSegment()?.subsegments.find((subsegment) =>
    // Find by trace id header. Parent is the subsegment id.
    subsegment.id === extractParentIdFromTraceHeader(args.request?.headers['X-Amzn-Trace-Id']));
  const { input } = args;

  segment?.addMetadata('Input', input);

  return next(args);
};

const extractParentIdFromTraceHeader = (traceHeader) => {
  // Trace header of the form: Root=ROOT_ID;Parent=PARENT_ID;Sampled=0;Lineage=OTHER_ID:0
  const match = traceHeader?.match(/^.+(Parent=[a-zA-Z0-9]+);.*$/);
  return match?.[1].split('=')[1];
};

const applyInputCaptureMiddleware = (capturedClient) => {
  capturedClient.middlewareStack.remove(INPUT_CAPTURE_MIDDLEWARE_NAME);
  capturedClient.middlewareStack.use({
    applyToStack: (stack) => stack.addRelativeTo(inputCaptureMiddleware, {
      name: INPUT_CAPTURE_MIDDLEWARE_NAME,
      relation: 'after',
      toMiddleware: 'XRaySDKInstrumentation',
    }),
  });
};

/**
 * Clear pipeline segments before an invocation.
 */
export const clearPipelineSegments = () => {
  pipelineSegments = {};
};

export const getPipelineSegments = () => pipelineSegments;

/**
 * Starts a segment for a particular pipeline by id. Only start 1 segment
 * per pipeline id. Return segment.
 */
export const startPipelineSegment = (pipelineId) => {
  if (!pipelineSegments[pipelineId]) {
    const segment = AWSXray.getSegment().addNewSubsegment(pipelineId);
    pipelineSegments[pipelineId] = segment;
  }
  return pipelineSegments[pipelineId];
};

/**
 * Terminate a segment.
 */
export const terminateSegment = (pipelineId) => {
  pipelineSegments[pipelineId]?.close();
};
