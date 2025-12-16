/* eslint-disable quote-props */
import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { ttl } from '../../../src/utils';

import {
  updateExpression,
  timestampCondition,
  pkCondition,
  updateDynamoDB,
  putDynamoDB,
  setValue,
  removeValue,
  addToSet,
  deleteFromSet,
  updateExpressionFromFragments,
  ifNotExists,
  setNestedValue,
  incrementBy,
  decrementBy,
  append,
  prepend,
  removeNestedValue,
} from '../../../src/sinks/dynamodb';

import Connector from '../../../src/connectors/dynamodb';

describe('sinks/dynamodb.js', () => {
  afterEach(sinon.restore);

  describe('utils', () => {
    it('should calculate ttl', () => {
      expect(ttl(1540454400000, 30)).to.equal(1543046400);
    });
  });

  describe('conditions', () => {
    it('should calculate timestampCondition', () => {
      expect(timestampCondition()).to.deep.equal({
        ConditionExpression:
          'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
      });
    });

    it('should calculate pkCondition', () => {
      expect(pkCondition()).to.deep.equal({
        ConditionExpression: 'attribute_not_exists(pk)',
      });
    });
  });

  describe('updateExpression', () => {
    it('should calculate updateExpression', () => {
      expect(
        updateExpression({
          id: '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
          name: 'Thing One',
          description: 'This is thing one.',
          status: undefined,
          status2: null,
          discriminator: 'thing',
          latched: true,
          ttl: ttl(1540454400000, 30),
          timestamp: 1540454400000,
        }),
      ).to.deep.equal({
        ExpressionAttributeNames: {
          '#description': 'description',
          '#discriminator': 'discriminator',
          '#id': 'id',
          '#latched': 'latched',
          '#name': 'name',
          // '#status': 'status',
          '#status2': 'status2',
          '#timestamp': 'timestamp',
          '#ttl': 'ttl',
        },
        ExpressionAttributeValues: {
          ':description': 'This is thing one.',
          ':discriminator': 'thing',
          ':id': '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
          ':latched': true,
          ':name': 'Thing One',
          // ':status': undefined,
          // ':status2': null,
          ':timestamp': 1540454400000,
          ':ttl': 1543046400,
        },
        UpdateExpression:
          'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp REMOVE #status2',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate updateExpression adding values to a set', () => {
      const result = updateExpression({
        tags: new Set(['a', 'b']),
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':tags': ['a', 'b'],
        },
        UpdateExpression: 'ADD #tags :tags',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate updateExpression removing values from a set', () => {
      const result = updateExpression({
        tags_delete: new Set(['x', 'y']),
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':tags_delete': ['x', 'y'],
        },
        UpdateExpression: 'DELETE #tags :tags_delete',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should wrap calculate updateExpression wrapping a delete set value in a set', () => {
      const result = updateExpression({
        tags_delete: 'x',
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':tags_delete': ['x'],
        },
        UpdateExpression: 'DELETE #tags :tags_delete',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate complex updateExpression using SET, REMOVE, ADD, and DELETE', () => {
      const result = updateExpression({
        id: '123',
        name: 'Complex Thing',
        description: null,
        tags: new Set(['blue', 'green']),
        categories: new Set(['a', 'b']),
        tags_delete: 'red',
        categories_delete: new Set(['x', 'y']),
        ignoredField: undefined,
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#id': 'id',
          '#name': 'name',
          '#description': 'description',
          '#tags': 'tags',
          '#categories': 'categories',
        },
        ExpressionAttributeValues: {
          ':id': '123',
          ':name': 'Complex Thing',
          ':tags': ['blue', 'green'],
          ':categories': ['a', 'b'],
          ':tags_delete': ['red'],
          ':categories_delete': ['x', 'y'],
        },
        UpdateExpression:
          'SET #id = :id, #name = :name REMOVE #description ADD #tags :tags, #categories :categories DELETE #tags :tags_delete, #categories :categories_delete',
        ReturnValues: 'ALL_NEW',
      });
    });
  });

  describe('operations', () => {
    it('should call update', (done) => {
      const stub = sinon.stub(Connector.prototype, 'update').resolves({});

      const uows = [
        {
          updateRequest: {
            Key: {
              pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
              sk: 'thing',
            },
          },
        },
        {
          updateRequest: undefined,
        },
      ];

      _(uows)
        .through(updateDynamoDB())
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(2);
          expect(stub).to.have.been.calledWith({
            Key: {
              pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
              sk: 'thing',
            },
          });
          expect(collected[0].updateResponse).to.deep.equal({});
        })
        .done(done);
    });

    it('should call put', (done) => {
      const stub = sinon.stub(Connector.prototype, 'put').resolves({});

      const uows = [
        {
          putRequest: {
            Item: {
              pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
              sk: 'thing',
            },
          },
        },
        {
          putRequest: undefined,
        },
      ];

      _(uows)
        .through(putDynamoDB())
        .collect()
        .tap((collected) => {
          // console.log(JSON.stringify(collected, null, 2));

          expect(collected.length).to.equal(2);
          expect(stub).to.have.been.calledWith({
            Item: {
              pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
              sk: 'thing',
            },
          });
          expect(collected[0].putResponse).to.deep.equal({});
        })
        .done(done);
    });
  });

  describe('updateExpressionFromFragments', () => {
    it('should calculate updateExpression from fragments', () => {
      expect(
        updateExpressionFromFragments({
          id: setValue('2f8ac025-d9e3-48f9-ba80-56487ddf0b89'),
          name: setValue('Thing One'),
          description: setValue('This is thing one.'),
          status: undefined,
          status2: removeValue(),
          discriminator: setValue('thing'),
          latched: setValue(true),
          ttl: setValue(ttl(1540454400000, 30)),
          timestamp: setValue(1540454400000),
        }),
      ).to.deep.equal({
        ExpressionAttributeNames: {
          '#description': 'description',
          '#discriminator': 'discriminator',
          '#id': 'id',
          '#latched': 'latched',
          '#name': 'name',
          // '#status': 'status',
          '#status2': 'status2',
          '#timestamp': 'timestamp',
          '#ttl': 'ttl',
        },
        ExpressionAttributeValues: {
          ':description': 'This is thing one.',
          ':discriminator': 'thing',
          ':id': '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
          ':latched': true,
          ':name': 'Thing One',
          // ':status': undefined,
          // ':status2': null,
          ':timestamp': 1540454400000,
          ':ttl': 1543046400,
        },
        UpdateExpression:
          'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp REMOVE #status2',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate updateExpression adding values to a set from fragments', () => {
      const result = updateExpressionFromFragments({
        tags: addToSet(new Set(['a', 'b'])),
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':tags__add': ['a', 'b'],
        },
        UpdateExpression: 'ADD #tags :tags__add',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate updateExpression deleting values from a set with fragments', () => {
      const result = updateExpressionFromFragments({
        tags: deleteFromSet(new Set(['x', 'y'])),
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':tags__delete': ['x', 'y'],
        },
        UpdateExpression: 'DELETE #tags :tags__delete',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate complex updateExpression using SET, REMOVE, ADD, and DELETE fragments', () => {
      const result = updateExpressionFromFragments({
        id: setValue('123'),
        name: setValue('Complex Thing'),
        description: removeValue(),
        'nested.removal': removeNestedValue(),
        categories: addToSet(new Set(['a', 'b'])),
        letters: deleteFromSet(new Set(['x', 'y'])),
        ignoredField: undefined,
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#id': 'id',
          '#name': 'name',
          '#description': 'description',
          '#categories': 'categories',
          '#letters': 'letters',
          '#nested': 'nested',
          '#removal': 'removal',
        },
        ExpressionAttributeValues: {
          ':id': '123',
          ':name': 'Complex Thing',
          ':categories__add': ['a', 'b'],
          ':letters__delete': ['x', 'y'],
        },
        UpdateExpression:
          'SET #id = :id, #name = :name REMOVE #description, #nested.#removal ADD #categories :categories__add DELETE #letters :letters__delete',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should calculate complex updateExpression using SET, REMOVE at index', () => {
      const result = updateExpressionFromFragments({
        topLevelValue: setValue('123', { atIndex: 1 }),
        'nested.value': setNestedValue(5, { atIndex: 2 }),
        topLevelRemoval: removeValue({ atIndex: 3 }),
        'nested.removal': removeNestedValue({ atIndex: 4 }),
      });

      expect(normalizeObj(result)).to.deep.equal({
        ExpressionAttributeNames: {
          '#topLevelValue': 'topLevelValue',
          '#nested': 'nested',
          '#value': 'value',
          '#topLevelRemoval': 'topLevelRemoval',
          '#removal': 'removal',
        },
        ExpressionAttributeValues: {
          ':topLevelValue': '123',
          ':nested.value': 5,
        },
        UpdateExpression:
          'SET #topLevelValue[1] = :topLevelValue, #nested.#value[2] = :nested.value REMOVE #topLevelRemoval[3], #nested.#removal[4]',
        ReturnValues: 'ALL_NEW',
      });
    });

    it('should resolve operands', () => {
      const result = updateExpressionFromFragments({
        createdAt: setValue(ifNotExists(1234567890)),
        'obj.nested.createdAt': setNestedValue(ifNotExists(1234567891)),
        incrementValue: setValue(incrementBy(1)),
        'obj.increment.value': setNestedValue(incrementBy(3)),
        decrementValue: setValue(decrementBy(5)),
        'obj.decrement.value': setNestedValue(decrementBy(7)),
        listAttributeA: setValue(append([1, 2, 3])),
        'list.nested.append': setNestedValue(append([4, 5, 6])),
        listAttributeB: setValue(prepend([7, 8, 9])),
        'list.nested.prepend': setNestedValue(prepend([10, 11, 12])),
      });

      expect(normalizeObj(result)).to.deep.equal({
        'ExpressionAttributeNames': {
          '#createdAt': 'createdAt',
          '#incrementValue': 'incrementValue',
          '#obj': 'obj',
          '#increment': 'increment',
          '#value': 'value',
          '#decrementValue': 'decrementValue',
          '#decrement': 'decrement',
          '#listAttributeA': 'listAttributeA',
          '#list': 'list',
          '#nested': 'nested',
          '#append': 'append',
          '#listAttributeB': 'listAttributeB',
          '#prepend': 'prepend',
        },
        'ExpressionAttributeValues': {
          ':createdAt': 1234567890,
          ':obj.nested.createdAt': 1234567891,
          ':incrementValue': 1,
          ':obj.increment.value': 3,
          ':decrementValue': 5,
          ':obj.decrement.value': 7,
          ':listAttributeA': [1, 2, 3],
          ':list.nested.append': [4, 5, 6],
          ':listAttributeB': [7, 8, 9],
          ':list.nested.prepend': [10, 11, 12],
        },
        'UpdateExpression': 'SET #createdAt = if_not_exists(#createdAt, :createdAt), #obj.#nested.#createdAt = if_not_exists(#obj.#nested.#createdAt), :obj.nested.createdAt, #incrementValue = #incrementValue + :incrementValue, #obj.#increment.#value = #obj.#increment.#value + :obj.increment.value, #decrementValue = #decrementValue - :decrementValue, #obj.#decrement.#value = #obj.#decrement.#value - :obj.decrement.value, #listAttributeA = list_append(#listAttributeA, :listAttributeA, #list.#nested.#append = list_append(#list.#nested.#append, :list.nested.append), #listAttributeB = list_append(:listAttributeB, #listAttributeB), #list.#nested.#prepend = list_append(:list.nested.prepend, #list.#nested.#prepend)',
        'ReturnValues': 'ALL_NEW',
      });
    });
  });
});

// Chai doesn't like sets...we can convert them to arrays to help it out.
const normalizeObj = (obj) =>
  JSON.parse(
    JSON.stringify(obj, (thisArg, value) =>
      (value instanceof Set ? [...value] : value)),
  );
