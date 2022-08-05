import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';

import {
  initialize, initializeFrom,
} from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { job } from '../../../src/flavors/job';

describe('flavors/job.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });

  afterEach(sinon.restore);

  it('should execute', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'scan').resolves({ Items: [{ pk: '1' }, { pk: '2' }] });

    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'job',
        },
        newImage: {
          pk: '1',
          sk: 'job',
          discriminator: 'job',
        },
      },
    ]);

    initialize({
      ...initializeFrom(rules),
    }, { ...defaultOptions, AES: false })
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);
        expect(collected[0].pipeline).to.equal('job1');
        expect(collected[0].scanRequest).to.be.deep.equal({
          ExclusiveStartKey: undefined,
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '11',
          },
        });
        expect(collected[0].emit).to.deep.equal({
          type: 'xyz',
          raw: {
            pk: '1',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'job1',
            skip: true,
          },
        });
      })
      .done(done);
  });
});

const rules = [
  {
    id: 'job1',
    eventType: 'job-created',
    flavor: job,
    filters: [() => true],
    toScanRequest: (uow) => ({
      ExpressionAttributeNames: {
        '#data': 'data',
      },
      ExpressionAttributeValues: {
        ':data': '11',
      },
    }),
    toEvent: (uow) => ({
      type: 'xyz',
      raw: uow.scanResponse.Item,
    }),
  },
];
