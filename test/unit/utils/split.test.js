import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import _ from 'highland';

import {
  splitObject,
} from '../../../src/utils';

describe('utils/split.js', () => {
  afterEach(sinon.restore);

  it('should split, defaults', (done) => {
    const queryResponse = [
      {
        id: '12340',
      },
      {
        id: '12341',
      },
      {
        id: '12342',
      },
      {
        id: '12343',
      },
      {
        id: '12344',
      },
    ];
    const uows = [
      {
        queryResponse,
      },
    ];

    _(uows)
      .through(splitObject({
        splitOn: 'queryResponse',
        debug: (msg, v) => console.log(msg, v),
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(5);
        expect(collected[0].queryResponse).to.deep.equal(queryResponse);
        expect(collected[0].splitOnTotal).to.equal(5);
        expect(collected[0].splitOnItemNumber).to.equal(1);
        expect(collected[0].split).to.deep.equal(queryResponse[0]);
        expect(collected[1].queryResponse).to.deep.equal(queryResponse);
        expect(collected[1].splitOnTotal).to.equal(5);
        expect(collected[1].splitOnItemNumber).to.equal(2);
        expect(collected[1].split).to.deep.equal(queryResponse[1]);
        expect(collected[2].queryResponse).to.deep.equal(queryResponse);
        expect(collected[2].splitOnTotal).to.equal(5);
        expect(collected[2].splitOnItemNumber).to.equal(3);
        expect(collected[2].split).to.deep.equal(queryResponse[2]);
        expect(collected[3].queryResponse).to.deep.equal(queryResponse);
        expect(collected[3].splitOnTotal).to.equal(5);
        expect(collected[3].splitOnItemNumber).to.equal(4);
        expect(collected[3].split).to.deep.equal(queryResponse[3]);
        expect(collected[4].queryResponse).to.deep.equal(queryResponse);
        expect(collected[4].splitOnTotal).to.equal(5);
        expect(collected[4].splitOnItemNumber).to.equal(5);
        expect(collected[4].split).to.deep.equal(queryResponse[4]);
      })
      .done(done);
  });

  it('should split, rename splitTargetField field', (done) => {
    const queryResponse = [
      {
        id: '12340',
      },
      {
        id: '12341',
      },
      {
        id: '12342',
      },
      {
        id: '12343',
      },
      {
        id: '12344',
      },
    ];
    const uows = [
      {
        queryResponse,
      },
    ];

    _(uows)
      .through(splitObject({
        splitOn: 'queryResponse',
        splitTargetField: 'idObject',
        debug: (msg, v) => console.log(msg, v),
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(5);
        expect(collected[0].queryResponse).to.deep.equal(queryResponse);
        expect(collected[0].splitOnTotal).to.equal(5);
        expect(collected[0].splitOnItemNumber).to.equal(1);
        expect(collected[0].split).to.be.undefined;
        expect(collected[0].idObject).to.deep.equal(queryResponse[0]);
        expect(collected[1].queryResponse).to.deep.equal(queryResponse);
        expect(collected[1].splitOnTotal).to.equal(5);
        expect(collected[1].splitOnItemNumber).to.equal(2);
        expect(collected[1].split).to.be.undefined;
        expect(collected[1].idObject).to.deep.equal(queryResponse[1]);
        expect(collected[2].queryResponse).to.deep.equal(queryResponse);
        expect(collected[2].splitOnTotal).to.equal(5);
        expect(collected[2].splitOnItemNumber).to.equal(3);
        expect(collected[2].split).to.be.undefined;
        expect(collected[2].idObject).to.deep.equal(queryResponse[2]);
        expect(collected[3].queryResponse).to.deep.equal(queryResponse);
        expect(collected[3].splitOnTotal).to.equal(5);
        expect(collected[3].splitOnItemNumber).to.equal(4);
        expect(collected[3].split).to.be.undefined;
        expect(collected[3].idObject).to.deep.equal(queryResponse[3]);
        expect(collected[4].queryResponse).to.deep.equal(queryResponse);
        expect(collected[4].splitOnTotal).to.equal(5);
        expect(collected[4].splitOnItemNumber).to.equal(5);
        expect(collected[4].split).to.be.undefined;
        expect(collected[4].idObject).to.deep.equal(queryResponse[4]);
      })
      .done(done);
  });

  it('should split, remove some fields', (done) => {
    const queryResponse = [
      {
        id: '12340',
      },
      {
        id: '12341',
      },
      {
        id: '12342',
      },
      {
        id: '12343',
      },
      {
        id: '12344',
      },
    ];
    const uows = [
      {
        queryResponse,
      },
    ];

    _(uows)
      .through(splitObject({
        splitOn: 'queryResponse',
        splitOnOmitFields: 'queryResponse',
        debug: (msg, v) => console.log(msg, v),
      }))
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));
        expect(collected.length).to.equal(5);
        expect(collected[0].queryResponse).to.be.undefined;
        expect(collected[0].splitOnTotal).to.equal(5);
        expect(collected[0].splitOnItemNumber).to.equal(1);
        expect(collected[0].split).to.deep.equal(queryResponse[0]);
        expect(collected[1].queryResponse).to.be.undefined;
        expect(collected[1].splitOnTotal).to.equal(5);
        expect(collected[1].splitOnItemNumber).to.equal(2);
        expect(collected[1].split).to.deep.equal(queryResponse[1]);
        expect(collected[2].queryResponse).to.be.undefined;
        expect(collected[2].splitOnTotal).to.equal(5);
        expect(collected[2].splitOnItemNumber).to.equal(3);
        expect(collected[2].split).to.deep.equal(queryResponse[2]);
        expect(collected[3].queryResponse).to.be.undefined;
        expect(collected[3].splitOnTotal).to.equal(5);
        expect(collected[3].splitOnItemNumber).to.equal(4);
        expect(collected[3].split).to.deep.equal(queryResponse[3]);
        expect(collected[4].queryResponse).to.be.undefined;
        expect(collected[4].splitOnTotal).to.equal(5);
        expect(collected[4].splitOnItemNumber).to.equal(5);
        expect(collected[4].split).to.deep.equal(queryResponse[4]);
      })
      .done(done);
  });
});
