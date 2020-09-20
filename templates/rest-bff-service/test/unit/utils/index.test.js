import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import {
  now, mapper, aggregateMapper, DEFAULT_OMIT_FIELDS,
} from '../../../src/utils';

describe('utils/index.js', () => {
  afterEach(sinon.restore);

  it('should test successful handle call', async () => {
    sinon.stub(Date, 'now').returns(1600144863435);
    expect(now()).to.equal(1600144863435);
  });

  it('should map an object', () => {
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
        f1: (v) => v.toUpperCase(),
        f9: (v) => 'else-coverage',
      },
    });

    expect(mappings({
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

  it('should map an aggregate object', () => {
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
      transform: { sk: (v) => v.split('|')[1] },
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

    const mapped = mappings([
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
