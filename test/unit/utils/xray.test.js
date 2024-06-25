import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWSXray from 'aws-xray-sdk-core';
import _ from 'highland';
import debug from 'debug';
import { throwFault, toPromise } from '../../../src/utils';
import { initialize, initializeFrom } from '../../../src/pipelines';

import CloudwatchConnector from '../../../src/connectors/cloudwatch';
import EventBridgeConnector from '../../../src/connectors/eventbridge';
import FirehoseConnector from '../../../src/connectors/firehose';
import KinesisConnector from '../../../src/connectors/kinesis';
import LambdaConnector from '../../../src/connectors/lambda';
import S3Connector from '../../../src/connectors/s3';
import SecretsMgrConnector from '../../../src/connectors/secretsmgr';
import SnsConnector from '../../../src/connectors/sns';
import SqsConnector from '../../../src/connectors/sqs';
import DynamoConnector from '../../../src/connectors/dynamodb';

const TEST_ROOT_SEGMENT = {
  id: 'test_root_segment',
  name: 'test root segment',
  start_time: 1719258954.364,
  end_time: undefined,
  in_progress: true,
  trace_id: '1-6679bdf7-f8a4fdf87d3277cb7f67c290',
  parent_id: null,

  // eslint-disable-next no-unused-vars
  addNewSubsegment: (segmtId) => {},
};

const TEST_SUBSEGMENT = {
  id: 'test_subsegment',
  name: 'test subsegment',
  start_time: 1719258954.364,
  end_time: undefined,
  in_progress: true,
  trace_id: '1-6679bdf7-f8a4fdf87d3277cb7f67c290',
  parent_id: 'test_root_segment',

  close: () => {},
};

describe('utils/xray.js', () => {
  afterEach(sinon.restore);

  it('should capture global promise and https on require.', () => {
    const promiseStub = sinon.stub(AWSXray, 'capturePromise').returns();
    const httpsStub = sinon.stub(AWSXray, 'captureHTTPsGlobal').returns();

    require('../../../src/utils/xray');

    expect(promiseStub).to.have.been.calledOnce;
    expect(httpsStub).to.have.been.calledOnce;
  });

  it('should capture sdk client', () => {
    const stub = sinon.stub(AWSXray, 'captureAWSv3Client');
    const { captureSdkClientTraces } = require('../../../src/utils/xray');
    const testClientObj = {};

    captureSdkClientTraces(testClientObj);

    expect(stub).to.have.been.calledOnceWith(testClientObj);
  });

  it('should start a segment when receiving a uow', () => {
    sinon.stub(AWSXray, 'capturePromise');
    sinon.stub(AWSXray, 'captureHTTPsGlobal');
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(TEST_SUBSEGMENT);

    const { startPipelineSegment, getPipelineSegments } = require('../../../src/utils/xray');
    const mappedUow = startPipelineSegment('test_subsegment')({ existingUow: true });

    expect(getPipelineSegments().test_subsegment).to.deep.eq(TEST_SUBSEGMENT);
    expect(mappedUow).to.deep.eq({
      traceContext: {
        xraySegment: TEST_SUBSEGMENT,
      },
      existingUow: true,
    });
  });

  it('should clear segments', () => {
    sinon.stub(AWSXray, 'capturePromise');
    sinon.stub(AWSXray, 'captureHTTPsGlobal');
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(TEST_SUBSEGMENT);

    const { startPipelineSegment, getPipelineSegments, clearPipelineSegments } = require('../../../src/utils/xray');
    startPipelineSegment('test_clear_subsegment')({ existingUow: true });

    expect(getPipelineSegments().test_clear_subsegment).to.deep.eq(TEST_SUBSEGMENT);

    clearPipelineSegments();

    expect(getPipelineSegments()).to.deep.eq({});
  });

  it('should terminate a segment at the end of the pipeline', async () => {
    sinon.stub(AWSXray, 'capturePromise');
    sinon.stub(AWSXray, 'captureHTTPsGlobal');
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    const subSegment = {
      ...TEST_SUBSEGMENT,
      close() {
        this.in_progress = false;
      },
    };
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(subSegment);

    const { startPipelineSegment, terminateSegment } = require('../../../src/utils/xray');

    await _([{ val: 1 }, { val: 2 }, { val: 3 }])
      .map(startPipelineSegment('test_terminate_segment'))
      // Should still be in progress for all processing between starting and finishing.
      .tap(() => expect(subSegment.in_progress).to.be.true)
      .through(terminateSegment('test_terminate_segment'))
      .collect()
      .toPromise(Promise);

    // Closed after terminate receives nil
    expect(subSegment.in_progress).to.be.false;
  });

  it('should handle error propagation in segment termination', async () => {
    sinon.stub(AWSXray, 'capturePromise');
    sinon.stub(AWSXray, 'captureHTTPsGlobal');
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    const subSegment = {
      ...TEST_SUBSEGMENT,
      close() {
        this.in_progress = false;
      },
    };
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(subSegment);

    const { startPipelineSegment, terminateSegment } = require('../../../src/utils/xray');

    await _([{ val: 1 }, { val: 2 }, { val: 3 }])
      .map(startPipelineSegment('test_segment_err_prop'))
      .map((uow) => {
        if (uow.val === 2) throwFault(uow)(new Error('Forced error'));
        return uow;
      })
      // Should still be in progress for all processing between starting and finishing.
      .tap(() => expect(subSegment.in_progress).to.be.true)

      .through(terminateSegment('test_segment_err_prop'))
      .collect()
      .errors((err) => {
        expect(err.message).to.eq('Forced error');
      })
      .toPromise(Promise);

    // Closed after terminate receives nil
    expect(subSegment.in_progress).to.be.false;
  });

  describe('pipeline integration', () => {
    it('bypasses xray if not enabled', async () => {
      const xrayIntegration = require('../../../src/utils/xray');
      const startStub = sinon.stub(xrayIntegration, 'startPipelineSegment');
      const endStub = sinon.stub(xrayIntegration, 'terminateSegment');
      const clearStub = sinon.stub(xrayIntegration, 'clearPipelineSegments');

      const pipeline = initialize(initializeFrom([{
        id: 'TestPipeline',
        flavor: (opt) => (s) =>
          s.map((uow) => ({ ...uow, val: uow.val * 2 })),
      }]), {
        xrayEnabled: false,
        publish: (s) => s,
      }).assemble(_([{ val: 1 }]), false);

      await pipeline.through(toPromise);

      expect(startStub).to.not.have.been.called;
      expect(endStub).to.not.have.been.called;
      expect(clearStub).to.not.have.been.called;
    });

    it('issues calls to integration if enabled', async () => {
      const xrayIntegration = require('../../../src/utils/xray');
      const startStub = sinon.stub(xrayIntegration, 'startPipelineSegment').returns((uow) => uow);
      const endStub = sinon.stub(xrayIntegration, 'terminateSegment').returns((s) => s);
      const clearStub = sinon.spy(xrayIntegration, 'clearPipelineSegments');

      const pipeline = initialize(initializeFrom([{
        id: 'TestPipeline',
        flavor: (opt) => (s) =>
          s.map((uow) => ({ ...uow, val: uow.val * 2 })),
      }]), {
        xrayEnabled: true,
        publish: (s) => s,
      }).assemble(_([{ val: 1 }]), false);

      await pipeline.through(toPromise);

      expect(startStub).to.have.been.calledWith('TestPipeline');
      expect(endStub).to.have.been.calledWith('TestPipeline');
      expect(clearStub).to.have.been.called;
    });
  });

  describe('connector integration', () => {
    const validateConnector = (klass, called) => {
      const xrayIntegration = require('../../../src/utils/xray');
      const captureStub = sinon.stub(xrayIntegration, 'captureSdkClientTraces');

      const connector = new klass({ debug: debug('test'), xrayEnabled: called });

      if (called) {
        expect(captureStub).to.have.been.called;
      } else {
        expect(captureStub).to.not.have.been.called;
      }
    };

    describe('cloudwatch', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(CloudwatchConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(CloudwatchConnector, false);
      });
    });

    describe('eventbridge', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(EventBridgeConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(EventBridgeConnector, false);
      });
    });

    describe('firehose', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(FirehoseConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(FirehoseConnector, false);
      });
    });

    describe('kinesis', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(KinesisConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(KinesisConnector, false);
      });
    });

    describe('lambda', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(LambdaConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(LambdaConnector, false);
      });
    });

    describe('s3', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(S3Connector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(S3Connector, false);
      });
    });

    describe('secretsmgr', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(SecretsMgrConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(SecretsMgrConnector, false);
      });
    });

    describe('sns', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(SnsConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(SnsConnector, false);
      });
    });

    describe('sqs', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(SqsConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(SqsConnector, false);
      });
    });

    describe('dynamodb', () => {
      it('integrates with connector if enabled', () => {
        validateConnector(DynamoConnector, true);
      });

      it('does not integrate with connector if not enabled', () => {
        validateConnector(DynamoConnector, false);
      });
    });
  });
});
