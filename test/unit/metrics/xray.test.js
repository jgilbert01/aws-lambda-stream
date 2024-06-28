import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import AWSXray from 'aws-xray-sdk-core';
import _ from 'highland';
import debug from 'debug';
import nock from 'nock';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { toPromise } from '../../../src/utils';
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
import * as metrics from '../../../src/metrics';

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

  close() {
    this.in_progress = false;
  },
};

describe('utils/xray.js', () => {
  afterEach(sinon.restore);

  it('should capture global promise and https on require.', () => {
    const promiseStub = sinon.stub(AWSXray, 'capturePromise').returns();
    const httpsStub = sinon.stub(AWSXray, 'captureHTTPsGlobal').returns();

    require('../../../src/metrics/xray');

    expect(promiseStub).to.have.been.calledOnce;
    expect(httpsStub).to.have.been.calledOnce;
  });

  it('should capture sdk client', () => {
    const spy = sinon.spy(AWSXray, 'captureAWSv3Client');
    const { captureSdkClientTraces } = require('../../../src/metrics/xray');
    const ddbBase = new DynamoDBClient({});
    const ddbDocClient = DynamoDBDocumentClient.from(ddbBase);

    captureSdkClientTraces(ddbDocClient);

    expect(spy).to.have.been.calledOnceWith(ddbDocClient);
  });

  it('should start a segment', () => {
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(TEST_SUBSEGMENT);

    const { startPipelineSegment, getPipelineSegments } = require('../../../src/metrics/xray');
    startPipelineSegment('test_subsegment');

    expect(getPipelineSegments().test_subsegment).to.deep.eq(TEST_SUBSEGMENT);
  });

  it('should terminate a segment', () => {
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns({ ...TEST_SUBSEGMENT });

    const { startPipelineSegment, getPipelineSegments, terminateSegment } = require('../../../src/metrics/xray');

    startPipelineSegment('test_subsegment');
    expect(getPipelineSegments().test_subsegment.in_progress).to.be.true;

    terminateSegment('test_subsegment');
    expect(getPipelineSegments().test_subsegment.in_progress).to.be.false;
  });

  it('should clear segments', () => {
    sinon.stub(AWSXray, 'capturePromise');
    sinon.stub(AWSXray, 'captureHTTPsGlobal');
    sinon.stub(AWSXray, 'getSegment').returns(TEST_ROOT_SEGMENT);
    sinon.stub(TEST_ROOT_SEGMENT, 'addNewSubsegment').returns(TEST_SUBSEGMENT);

    const { startPipelineSegment, getPipelineSegments, clearPipelineSegments } = require('../../../src/metrics/xray');
    startPipelineSegment('test_clear_subsegment');

    expect(getPipelineSegments().test_clear_subsegment).to.deep.eq(TEST_SUBSEGMENT);

    clearPipelineSegments();

    expect(getPipelineSegments()).to.deep.eq({});
  });

  describe('pipeline integration', () => {
    it('bypasses xray if not enabled', async () => {
      const xrayIntegration = require('../../../src/metrics/xray');
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
      const xrayIntegration = require('../../../src/metrics/xray');
      const startStub = sinon.stub(xrayIntegration, 'startPipelineSegment').returns((uow) => uow);
      const endStub = sinon.stub(xrayIntegration, 'terminateSegment');
      const clearStub = sinon.spy(xrayIntegration, 'clearPipelineSegments');

      const opt = { xrayEnabled: true, publish: (s) => s, metrics };

      const pipeline = initialize(initializeFrom([{
        id: 'TestPipeline',
        flavor: () => (s) =>
          s.map((uow) => ({ ...uow, val: uow.val * 2 })),
      }]), opt).assemble(_([
        {
          val: 1,
          metrics: metrics.startUow(Date.now(), 1),
        },
      ]), false);

      await pipeline.through(metrics.toPromiseWithMetrics(opt));

      expect(startStub).to.have.been.calledWith('TestPipeline');
      expect(endStub).to.have.been.calledWith('TestPipeline');
      expect(clearStub).to.have.been.called;
    });
  });

  describe('connector integration', () => {
    const validateConnector = async (klass, called, sendResponse = {}) => {
      const xrayIntegration = require('../../../src/metrics/xray');
      const captureStub = sinon.stub(xrayIntegration, 'captureSdkClientTraces');

      const connector = new klass({ debug: debug('test'), xrayEnabled: called, metrics });
      sinon.stub(connector.client, 'send');
      await connector._executeCommand({});

      if (called) {
        expect(captureStub).to.have.been.called;
      } else {
        expect(captureStub).to.not.have.been.called;
      }
    };

    describe('cloudwatch', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(CloudwatchConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(CloudwatchConnector, false);
      });
    });

    describe('eventbridge', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(EventBridgeConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(EventBridgeConnector, false);
      });
    });

    describe('firehose', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(FirehoseConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(FirehoseConnector, false);
      });
    });

    describe('kinesis', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(KinesisConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(KinesisConnector, false);
      });
    });

    describe('lambda', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(LambdaConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(LambdaConnector, false);
      });
    });

    describe('s3', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(S3Connector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(S3Connector, false);
      });
    });

    describe('secretsmgr', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(SecretsMgrConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(SecretsMgrConnector, false);
      });
    });

    describe('sns', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(SnsConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(SnsConnector, false);
      });
    });

    describe('sqs', () => {
      it('integrates with connector if enabled', async () => {
        await validateConnector(SqsConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(SqsConnector, false);
      });
    });

    describe('dynamodb', () => {
      // afterEach(nock.restore);

      it('integrates with connector if enabled', async () => {
        await validateConnector(DynamoConnector, true);
      });

      it('does not integrate with connector if not enabled', async () => {
        await validateConnector(DynamoConnector, false);
      });

      it('injects middleware to capture input', async () => {
        nock(`https://dynamodb.${process.env.AWS_REGION}.amazonaws.com`)
          .post('/')
          .reply(200, { Responses: { 'test-table': [] }, UnprocessedKeys: {} });

        AWSXray.enableAutomaticMode();
        const namespace = AWSXray.getNamespace();
        namespace.enter(namespace.createContext());
        AWSXray.setSegment(new AWSXray.Segment('Root'));

        const connector = new DynamoConnector({
          debug: debug('test'), xrayEnabled: true, tableName: 'test-table', metrics,
        });
        await connector.batchGet({
          RequestItems: {
            'test-table': {
              Keys: [
                {
                  pk: 'test-pk',
                  sk: 'test-sk',
                },
              ],
            },
          },
        });

        const segment = AWSXray.getSegment();
        const subsegment = segment.subsegments[0];
        expect(subsegment.metadata).to.deep.eq({
          default: {
            Input: {
              RequestItems: {
                'test-table': {
                  Keys: [
                    {
                      pk: { S: 'test-pk' },
                      sk: { S: 'test-sk' },
                    },
                  ],
                },
              },
            },
          },
        });
      });
    });
  });
});
