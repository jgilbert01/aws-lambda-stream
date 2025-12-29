import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import { ttl } from '../../../src/utils';

import {
  updateExpression, timestampCondition, pkCondition,
  updateDynamoDB, putDynamoDB,
} from '../../../src/sinks/dynamodb';

import Connector from '../../../src/connectors/dynamodb';

describe('sinks/dynamodb.js', () => {
  afterEach(sinon.restore);

  it('should calculate ttl', () => {
    expect(ttl(1540454400000, 30)).to.equal(1543046400);
  });

  it('should calculate updateExpression', () => {
    expect(updateExpression({
      'id': '2f8ac025-d9e3-48f9-ba80-56487ddf0b89',
      'name': 'Thing One',
      'description': 'This is thing one.',
      'status': undefined,
      'status2': null,
      'discriminator': 'thing',
      'latched': true,
      'ttl': ttl(1540454400000, 30),
      'timestamp': 1540454400000,
      'some unsafe att name': true,
      'some unsafe att name to delete': null,
    })).to.deep.equal({
      ExpressionAttributeNames: {
        '#description': 'description',
        '#discriminator': 'discriminator',
        '#id': 'id',
        '#latched': 'latched',
        '#name': 'name',
        '#some_x20_unsafe_x20_att_x20_name': 'some unsafe att name',
        '#some_x20_unsafe_x20_att_x20_name_x20_to_x20_delete': 'some unsafe att name to delete',
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
        ':some_x20_unsafe_x20_att_x20_name': true,
        ':timestamp': 1540454400000,
        ':ttl': 1543046400,
      },
      UpdateExpression: 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #latched = :latched, #ttl = :ttl, #timestamp = :timestamp, #some_x20_unsafe_x20_att_x20_name = :some_x20_unsafe_x20_att_x20_name REMOVE #status2, #some_x20_unsafe_x20_att_x20_name_x20_to_x20_delete',
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

  it('should calculate updateExpression removing values from a set when attribute names have illegal characters if used as an alias', () => {
    const result = updateExpression({
      'some|tags_delete': new Set(['x', 'y']),
      'a-b': true,
      'a--b': false,
      'a|b': 1,
    });

    expect(normalizeObj(result)).to.deep.equal({
      ExpressionAttributeNames: {
        '#some_x7c_tags': 'some|tags',
        '#a_x2d_b': 'a-b',
        '#a_x2d__x2d_b': 'a--b',
        '#a_x7c_b': 'a|b',
      },
      ExpressionAttributeValues: {
        ':some_x7c_tags_delete': [
          'x',
          'y',
        ],
        ':a_x2d_b': true,
        ':a_x2d__x2d_b': false,
        ':a_x7c_b': 1,
      },
      UpdateExpression: 'SET #a_x2d_b = :a_x2d_b, #a_x2d__x2d_b = :a_x2d__x2d_b, #a_x7c_b = :a_x7c_b DELETE #some_x7c_tags :some_x7c_tags_delete',
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
      UpdateExpression: 'SET #id = :id, #name = :name REMOVE #description ADD #tags :tags, #categories :categories DELETE #tags :tags_delete, #categories :categories_delete',
      ReturnValues: 'ALL_NEW',
    });
  });

  it('should calculate timestampCondition', () => {
    expect(timestampCondition()).to.deep.equal({
      ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
    });
  });

  it('should calculate pkCondition', () => {
    expect(pkCondition()).to.deep.equal({
      ConditionExpression: 'attribute_not_exists(pk)',
    });
  });

  it('should call update', (done) => {
    const stub = sinon.stub(Connector.prototype, 'update').resolves({});

    const uows = [{
      updateRequest: {
        Key: {
          pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
          sk: 'thing',
        },
      },
    }, {
      updateRequest: undefined,
    }];

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

    const uows = [{
      putRequest: {
        Item: {
          pk: '72363701-fd38-4887-94b9-e8f8aecf6208',
          sk: 'thing',
        },
      },
    }, {
      putRequest: undefined,
    }];

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

// Chai doesn't like sets...we can convert them to arrays to help it out.
const normalizeObj = (obj) =>
  JSON.parse(JSON.stringify(obj, (thisArg, value) =>
    (value instanceof Set ? [...value] : value)));
