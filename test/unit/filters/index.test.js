import 'mocha';
import { expect } from 'chai';

import { filterOnEventType, filterOnContent } from '../../../src/filters';

describe('filters/index.js', () => {
  it('should filter by type', () => {
    const rule = {
      eventType: 't1',
    };

    expect(filterOnEventType(rule, { event: { type: 't1' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 't2' } })).to.be.false;
  });

  it('should filter by regex', () => {
    const rule = {
      eventType: /t(1|3)/,
      // eventType: /t.*/,
    };

    expect(filterOnEventType(rule, { event: { type: 't1' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 't3' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 'x1' } })).to.be.false;
  });

  it('should filter by array of types', () => {
    const rule = {
      eventType: ['t1', 't2'],
    };

    expect(filterOnEventType(rule, { event: { type: 't1' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 't2' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 't3' } })).to.be.false;
  });

  it('should filter by function', () => {
    const rule = {
      eventType: (type) => type.startsWith('t'),
    };

    expect(filterOnEventType(rule, { event: { type: 't1' } })).to.be.true;
    expect(filterOnEventType(rule, { event: { type: 'x1' } })).to.be.false;
  });

  it('should throw', () => {
    const rule = {
      eventType: false,
    };

    expect(() => filterOnEventType(rule, {})).to.throw('Rule: undefined, has improperly configured eventType filter. Must be a string, array of string, regex or function.');
  });

  it('should filter on content', () => {
    const rule = {
      filters: [
        (uow, r) => uow.event.tags.f === r.value,
      ],
      value: 'v1',
    };

    expect(filterOnContent(rule, { event: { tags: { f: 'v1' } } })).to.be.true;
    expect(filterOnContent(rule, { event: { tags: { f: 'v2' } } })).to.be.false;
  });

  it('should filter on compound content', () => {
    const errorFilter = (uow) => { uow.isSkipped = true; };
    const rule = {
      filters: [
        (uow, r) => uow.event.tags.f1 === r.value,
        (uow, r) => uow.event.tags.f2 === r.value,
      ],
      errorFilter,
      value: 'v1',
    };

    expect(filterOnContent(rule, { event: { tags: { f1: 'v1', f2: 'v1' } } })).to.be.true;
    const event = { event: { tags: { f1: 'v2', f2: 'v1' } } };
    expect(filterOnContent(rule, event)).to.be.false;
    expect(event.isSkipped).to.be.true;
    expect(filterOnContent(rule, { event: { tags: { f1: 'v1', f2: 'v2' } } })).to.be.false;
  });
});
