import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import Promise from 'bluebird';

import {
  initialize, initializeFrom,
} from '../../../src';

import { defaultOptions } from '../../../src/utils/opt';
import { toDynamodbRecords, fromDynamodb } from '../../../src/from/dynamodb';
import { DynamoDBConnector, EventBridgeConnector } from '../../../src/connectors';

import { evaluate } from '../../../src/flavors/evaluate';

describe('flavors/evaluate.js', () => {
  beforeEach(() => {
    sinon.stub(EventBridgeConnector.prototype, 'putEvents').resolves({ FailedEntryCount: 0 });
  });

  afterEach(sinon.restore);

  it('should execute simple rules', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1548967023,
        keys: {
          pk: '1',
          sk: 'EVENT',
        },
        newImage: {
          pk: '1',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '11',
          event: {
            id: '1',
            type: 'e1',
            timestamp: 1548967022000,
            partitionKey: '11',
            thing: {
              id: '11',
              name: 'Thing One',
              description: 'This is thing one',
            },
          },
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '2',
          sk: 'EVENT',
        },
        newImage: {
          pk: '2',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '22',
          event: {
            id: '2',
            type: 'e2',
            timestamp: 1548967022000,
            partitionKey: '22',
            thing: {
              id: '22',
              name: 'Thing Two',
              description: 'This is thing two',
            },
          },
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '3',
          sk: 'EVENT',
        },
        newImage: {
          pk: '3',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '33',
          event: {
            id: '3',
            type: 'e3',
            timestamp: 1548967022000,
            partitionKey: '33',
            thing: {
              id: '33',
              name: 'Thing Three',
              description: 'This is thing three',
            },
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(RULES),
    }, defaultOptions)
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(4);

        expect(collected[0].pipeline).to.equal('eval1-basic-emit');
        expect(collected[0].event.type).to.equal('e1');
        expect(collected[0].meta).to.deep.equal({
          id: '0',
          pk: '1',
          data: '11',
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: undefined,
          correlationKey: '11',
          correlation: false,
          suffix: undefined,
        });
        expect(collected[0].emit).to.deep.equal({
          id: '0.eval1-basic-emit',
          type: 'e111',
          timestamp: 1548967022000,
          partitionKey: '11',
          thing: {
            id: '11',
            name: 'Thing One',
            description: 'This is thing one',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval1-basic-emit',
            skip: true,
          },
          triggers: [
            {
              id: '1',
              type: 'e1',
              timestamp: 1548967022000,
            },
          ],
        });

        expect(collected[1].pipeline).to.equal('eval2-single-emit');
        expect(collected[1].event.type).to.equal('e2');
        expect(collected[1].emit).to.deep.equal({
          id: '1.eval2-single-emit',
          type: 'e222',
          timestamp: 1548967022000,
          partitionKey: '22',
          thing: {
            id: '22',
            name: 'Thing Two',
            description: 'This is thing two',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval2-single-emit',
            skip: true,
          },
          triggers: [
            {
              id: '2',
              type: 'e2',
              timestamp: 1548967022000,
            },
          ],
        });

        expect(collected[2].pipeline).to.equal('eval3-multi-emit');
        expect(collected[2].event.type).to.equal('e3');
        expect(collected[2].emit).to.deep.equal({
          id: '2.eval3-multi-emit.1',
          type: 'e333.1',
          timestamp: 1548967022000,
          partitionKey: '33',
          thing: {
            id: '33',
            name: 'Thing Three',
            description: 'This is thing three',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval3-multi-emit',
            skip: true,
          },
          triggers: [
            {
              id: '3',
              type: 'e3',
              timestamp: 1548967022000,
            },
          ],
        });

        expect(collected[3].emit.id).to.equal('2.eval3-multi-emit.2');
        expect(collected[3].emit.type).to.equal('e333.2');
      })
      .done(done);
  });

  it('should execute complex rules', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'query').callsFake((params) => {
      const ck = params.ExpressionAttributeValues[':data'];
      const resp = {
        44: [{ event: { id: '4', type: 'e4', timestamp: 1548967022000 } }],
        55: [{ event: { id: '5', type: 'e5', timestamp: 1548967022000 } }],
      };
      return Promise.resolve(resp[ck]);
    });

    const events = toDynamodbRecords([
      {
        timestamp: 1548967023,
        keys: {
          pk: '4',
          sk: 'EVENT',
        },
        newImage: {
          pk: '4',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '44',
          event: {
            id: '4',
            type: 'e4',
            timestamp: 1548967022000,
            partitionKey: '44',
            thing: {
              id: '44',
              name: 'Thing Four',
              description: 'This is thing four',
            },
          },
        },
      },
      {
        timestamp: 1548967023,
        keys: {
          pk: '5',
          sk: 'EVENT',
        },
        newImage: {
          pk: '5',
          sk: 'EVENT',
          discriminator: 'EVENT',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          data: '55',
          event: {
            id: '5',
            type: 'e5',
            timestamp: 1548967022000,
            partitionKey: '55',
            thing: {
              id: '55',
              name: 'Thing Five',
              description: 'This is thing five',
            },
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(RULES),
    }, defaultOptions)
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[0].pipeline).to.equal('eval4');
        expect(collected[0].event.type).to.equal('e4');
        expect(collected[0].meta).to.deep.equal({
          id: '0',
          pk: '4',
          data: '44',
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: undefined,
          correlationKey: '44',
          correlation: false,
          suffix: undefined,
        });
        expect(collected[0].queryRequest).to.deep.equal({
          IndexName: 'DataIndex',
          KeyConditionExpression: '#data = :data',
          ExpressionAttributeNames: {
            '#data': 'data',
          },
          ExpressionAttributeValues: {
            ':data': '44',
          },
        });
        expect(collected[0].correlated).to.deep.equal([
          { id: '4', type: 'e4', timestamp: 1548967022000 },
        ]);
        expect(collected[0].emit).to.deep.equal({
          id: '0.eval4',
          type: 'e444',
          timestamp: 1548967022000,
          partitionKey: '44',
          thing: {
            id: '44',
            name: 'Thing Four',
            description: 'This is thing four',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval4',
            skip: true,
          },
          triggers: [
            {
              id: '4',
              type: 'e4',
              timestamp: 1548967022000,
            },
          ],
        });

        expect(collected[1].pipeline).to.equal('eval5');
        expect(collected[1].event.type).to.equal('e5');
        expect(collected[1].emit.triggers).to.deep.equal([
          {
            id: '51',
            type: 'e51',
            timestamp: 1548967022000,
          },
          {
            id: '52',
            type: 'e52',
            timestamp: 1548967022000,
          },
        ]);
      })
      .done(done);
  });

  it('should execute correlation rules', (done) => {
    sinon.stub(DynamoDBConnector.prototype, 'query').callsFake((params) => {
      const ck = params.ExpressionAttributeValues[':pk'];
      const resp = {
        '66': [{ event: { id: '66', type: 'e66', timestamp: 1548967022000 } }],
        '77.seven': [{ event: { id: '77', type: 'e77', timestamp: 1548967022000 } }],
      };
      return Promise.resolve(resp[ck]);
    });

    const events = toDynamodbRecords([
      // match - no suffix
      {
        timestamp: 1548967023,
        keys: {
          pk: '66',
          sk: '6',
        },
        newImage: {
          pk: '66',
          sk: '6',
          discriminator: 'CORREL',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          event: {
            id: '6',
            type: 'e6',
            timestamp: 1548967022000,
            partitionKey: '66',
            thing: {
              id: '66',
              name: 'Thing Six',
              description: 'This is thing six',
            },
          },
        },
      },
      // match - equal suffix
      {
        timestamp: 1548967023,
        keys: {
          pk: '77.seven',
          sk: '7',
        },
        newImage: {
          pk: '77.seven',
          sk: '7',
          discriminator: 'CORREL',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          suffix: 'seven',
          event: {
            id: '7',
            type: 'e7',
            timestamp: 1548967022000,
            partitionKey: '77',
            thing: {
              id: '77',
              name: 'Thing Seven',
              description: 'This is thing seven',
            },
          },
        },
      },
      // no match - missing suffix
      {
        timestamp: 1548967023,
        keys: {
          pk: '77',
          sk: '7',
        },
        newImage: {
          pk: '77',
          sk: '7',
          discriminator: 'CORREL',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          suffix: undefined,
          event: {
            type: 'e7',
          },
        },
      },
      // no match - wrong suffix
      {
        timestamp: 1548967023,
        keys: {
          pk: '77.seventy',
          sk: '7',
        },
        newImage: {
          pk: '77.seventy',
          sk: '7',
          discriminator: 'CORREL',
          timestamp: 1548967022000,
          sequenceNumber: '0',
          ttl: 1551818222,
          suffix: 'seventy',
          event: {
            type: 'e7',
          },
        },
      },
    ]);

    initialize({
      ...initializeFrom(RULES),
    }, defaultOptions)
      .assemble(fromDynamodb(events), false)
      .collect()
      // .tap((collected) => console.log(JSON.stringify(collected, null, 2)))
      .tap((collected) => {
        expect(collected.length).to.equal(2);

        expect(collected[0].pipeline).to.equal('eval6');
        expect(collected[0].event.type).to.equal('e6');
        expect(collected[0].meta).to.deep.equal({
          id: '0',
          pk: '66',
          data: undefined,
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: undefined,
          correlationKey: '66',
          correlation: true,
          suffix: undefined,
        });
        expect(collected[0].queryRequest).to.deep.equal({
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: {
            '#pk': 'pk',
          },
          ExpressionAttributeValues: {
            ':pk': '66',
          },
          ConsistentRead: true,
        });
        expect(collected[0].correlated).to.deep.equal([
          { id: '66', type: 'e66', timestamp: 1548967022000 },
        ]);
        expect(collected[0].emit).to.deep.equal({
          id: '0.eval6',
          type: 'e666',
          timestamp: 1548967022000,
          partitionKey: '66',
          thing: {
            id: '66',
            name: 'Thing Six',
            description: 'This is thing six',
          },
          tags: {
            account: 'undefined',
            region: 'us-west-2',
            stage: 'undefined',
            source: 'undefined',
            functionname: 'undefined',
            pipeline: 'eval6',
            skip: true,
          },
          triggers: [
            {
              id: '66',
              type: 'e66',
              timestamp: 1548967022000,
            },
          ],
        });

        expect(collected[1].pipeline).to.equal('eval7');
        expect(collected[1].event.type).to.equal('e7');
        expect(collected[1].meta).to.deep.equal({
          id: '1',
          pk: '77.seven',
          data: undefined,
          sequenceNumber: '0',
          ttl: 1551818222,
          expire: undefined,
          correlationKey: '77.seven',
          correlation: true,
          suffix: 'seven',
        });
        expect(collected[1].queryRequest).to.deep.equal({
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: {
            '#pk': 'pk',
          },
          ExpressionAttributeValues: {
            ':pk': '77.seven',
          },
          ConsistentRead: true,
        });
        expect(collected[1].correlated).to.deep.equal([
          { id: '77', type: 'e77', timestamp: 1548967022000 },
        ]);
        expect(collected[1].emit.triggers).to.deep.equal([
          {
            id: '7',
            type: 'e7',
            timestamp: 1548967022000,
          },
        ]);
      })
      .done(done);
  });
});

const RULES = [
  {
    id: 'eval1-basic-emit',
    flavor: evaluate,
    eventType: 'e1',
    emit: 'e111',
  },
  {
    id: 'eval2-single-emit',
    flavor: evaluate,
    eventType: 'e2',
    emit: (uow, rule, template) => ({
      ...template,
      type: 'e222',
      thing: uow.event.thing,
    }),
  },
  {
    id: 'eval3-multi-emit',
    flavor: evaluate,
    eventType: 'e3',
    emit: (uow, rule, template) => ([
      {
        ...template,
        id: `${template.id}.1`,
        type: 'e333.1',
        thing: uow.event.thing,
      },
      {
        ...template,
        id: `${template.id}.2`,
        type: 'e333.2',
        thing: uow.event.thing,
      },
    ]),
  },

  {
    id: 'eval4',
    flavor: evaluate,
    eventType: 'e4',
    expression: (uow) => true,
    emit: 'e444',
  },
  {
    id: 'eval5',
    flavor: evaluate,
    eventType: 'e5',
    expression: (uow) => [{
      id: '51',
      type: 'e51',
      timestamp: 1548967022000,
    },
    {
      id: '52',
      type: 'e52',
      timestamp: 1548967022000,
    }],
    emit: 'e555',
  },

  {
    id: 'eval6',
    flavor: evaluate,
    eventType: 'e6',
    expression: (uow) => uow.correlated.find((e) => e.type === 'e66'),
    emit: 'e666',
  },
  {
    id: 'eval7',
    flavor: evaluate,
    eventType: 'e7',
    correlationKeySuffix: 'seven',
    expression: (uow) => true,
    emit: 'e777',
  },
];
