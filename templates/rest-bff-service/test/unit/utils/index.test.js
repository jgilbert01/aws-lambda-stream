import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  now, mapper, aggregateMapper, DEFAULT_OMIT_FIELDS, sortKeyTransform, deletedFilter, getUsername, getClaims, getUserGroups, forRole,
} from '../../../src/utils';

describe('utils/index.js', () => {
  afterEach(sinon.restore);

  it('should test successful handle call', async () => {
    sinon.stub(Date, 'now').returns(1600144863435);
    expect(now()).to.equal(1600144863435);
  });

  it('should get claims', () => {
    expect(getClaims({
      authorizer: {
        claims: {
          'cognito:username': 'offlineContext_authorizer_principalId',
        },
      },
    })).to.deep.equal({
      'cognito:username': 'offlineContext_authorizer_principalId',
    });

    expect(getClaims({ authorizer: {} })).to.deep.equal({});
    expect(getClaims({})).to.deep.equal({});
  });

  it('should get username', () => {
    expect(getUsername({
      authorizer: {
        claims: {
          'cognito:username': 'offlineContext_authorizer_principalId',
        },
      },
    })).to.equal('offlineContext_authorizer_principalId');
  });

  it('should get user grouos', () => {
    expect(getUserGroups({
      authorizer: {
        claims: {
          'cognito:groups': 'r1,r2',
        },
      },
    })).to.deep.equal(['r1', 'r2']);
  });

  it('should check role to succeed', () => {
    const req = {
      requestContext: {
        authorizer: {
          claims: {
            'cognito:groups': 'r1,r2',
          },
        },
      },
    };
    const resp = {};
    const next = sinon.spy();

    forRole('r1')(req, resp, next);

    expect(next).to.have.been.calledWith();
  });

  it('should check role to raise error', () => {
    const req = {
      requestContext: {
        authorizer: {
          claims: {
            'cognito:groups': 'r3',
          },
        },
      },
    };
    const resp = {
      error: sinon.spy((data) => data),
    };
    const next = sinon.spy();

    forRole('r1')(req, resp, next);

    expect(resp.error).to.have.been.calledWith(401, 'Not Authorized');
  });

  it('should filter out soft deleted items', async () => {
    const items = [
      {
      },
      {
        deleted: undefined,
      },
      {
        deleted: null,
      },
      {
        deleted: true,
      },
      {
        deleted: false,
      },
    ]
      .filter(deletedFilter);
    expect(items.length).to.equal(4);
    expect(items).to.deep.equal([
      {
      },
      {
        deleted: undefined,
      },
      {
        deleted: null,
      },
      {
        deleted: false,
      },
    ]);
  });

  it('should map an object', async () => {
    const mappings = mapper({
      defaults: { f9: true },
      omit: [...DEFAULT_OMIT_FIELDS, 'f1'],
      rename: {
        pk: 'id',
        data: 'name',
        f1: 'f2',
        x1: 'else-coverage',
      },
      transform: {
        f1: async (v) => v.toUpperCase(),
        f9: (v) => 'else-coverage',
      },
    });

    expect(await mappings({
      pk: '1',
      sk: 'thing',
      data: 'thing0',
      f1: 'v1',
    })).to.deep.equal({
      id: '1',
      name: 'thing0',
      f2: 'V1',
      f9: true,
    });
  });

  it('should map an aggregate object', async () => {
    const mapper1 = mapper({
      rename: {
        pk: 'id',
        data: 'name',
      },
    });

    const mapper2 = mapper({
      rename: {
        sk: 'id',
        data: 'name',
      },
      transform: { sk: sortKeyTransform },
    });

    const mappings = aggregateMapper({
      aggregate: 'thing', // top level discriminator
      cardinality: { // per relationship
        one2many: 999,
        many2many: 999,
        one2one: 1,
      },
      mappers: { // per discriminator
        thing: mapper1,
        child: mapper2,
        peer: mapper2,
        associate: mapper2,
      },
    });

    const mapped = await mappings([
      {
        pk: '1',
        sk: 'many2many|1', // relationship name is 1st segment of sk
        discriminator: 'associate',
        data: 'associate1',
      },
      {
        pk: '1',
        sk: 'many2many|2',
        discriminator: 'associate',
        data: 'associate2',
      },
      {
        pk: '1',
        sk: 'one2many|1',
        discriminator: 'child',
        data: 'child1',
      },
      {
        pk: '1',
        sk: 'one2many|2',
        discriminator: 'child',
        data: 'child2',
      },
      {
        pk: '1',
        sk: 'one2many|3',
        discriminator: 'child',
        deleted: true,
      },
      {
        pk: '1',
        sk: 'one2one|1',
        discriminator: 'peer',
        data: 'peer',
      },
      {
        pk: '1',
        sk: 'thing',
        discriminator: 'thing',
        data: 'thing0',
        f1: 'v1',
      },
    ]);

    // console.log('mapped: %s', JSON.stringify(mapped, null, 2));

    expect(mapped).to.deep.equal({
      id: '1',
      name: 'thing0',
      f1: 'v1',
      one2many: [
        {
          id: '1',
          name: 'child1',
        },
        {
          id: '2',
          name: 'child2',
        },
      ],
      one2one: {
        id: '1',
        name: 'peer',
      },
      many2many: [
        {
          id: '1',
          name: 'associate1',
        },
        {
          id: '2',
          name: 'associate2',
        },
      ],
    });
  });
});
