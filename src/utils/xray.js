import AWSXray from 'aws-xray-sdk-core';
import debug from 'debug';
import _ from 'highland';

const log = debug('xray');

log('Initializing xray trace capture.');
AWSXray.capturePromise();
AWSXray.captureHTTPsGlobal(require('https'));

let pipelineSegments = {};

/**
 * Pass in parentSegment to explictly set the parent segment traces from this client
 * should be nested under. Otherwise, xray automode derives parent segment from
 * current context.
 */
export const captureSdkClientTraces = (sdkClient, traceContext = {}) =>
  // const parentSegment = traceContext?.xraySegment;
  // TODO - Set parent segment on capture. Automode in xray currently
  // prevents this.
  AWSXray.captureAWSv3Client(sdkClient);

/**
 * Clear pipeline segments before an invocation.
 */
export const clearPipelineSegments = () => {
  pipelineSegments = {};
};

export const getPipelineSegments = () => pipelineSegments;

/**
 * Starts a segment for a particular pipeline by id. Only start 1 segment
 * per pipeline id. Append to uow.
 */
export const startPipelineSegment = (pipelineId) => (uow) => {
  if (!pipelineSegments[pipelineId]) {
    const segment = AWSXray.getSegment().addNewSubsegment(pipelineId);
    pipelineSegments[pipelineId] = segment;
  }
  return {
    traceContext: {
      xraySegment: pipelineSegments[pipelineId],
    },
    ...uow,
  };
};

/**
 * Through stream to manage terminating the segment when the pipeline terminates.
 */
export const terminateSegment = (pipelineId) => (s) =>
  s.consume((err, x, push, next) => {
    // Normal operations unless we're at the end of the stream
    if (err) {
      push(err);
      next();
    } else if (x === _.nil) {
      // Terminate segment and continue.
      pipelineSegments[pipelineId]?.close();
      push(null, x);
    } else {
      push(null, x);
      next();
    }
  });
