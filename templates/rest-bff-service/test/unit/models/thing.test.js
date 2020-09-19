import 'mocha';
import { expect } from 'chai';
import * as sinon from 'sinon';
import debug from 'debug';

import * as utils from '../../../src/utils';
import Connector from '../../../src/connectors/dynamodb';
import {
  save, get, del, toEvent, toUpdateRequest,
} from '../../../src/models/thing';

describe('models/thing.js', () => {
  afterEach(() => {
    sinon.restore();
  });

  it('should save', async () => {
    sinon.stub(utils, 'now').returns(1600349040394);
    const stub = sinon.stub(Connector.prototype, 'update')
      .resolves({});

    const connector = new Connector(
      debug('db'),
      't1',
    );

    const data = await save(
      { connector },
      '00000000-0000-0000-0000-000000000000',
      {
        name: 'thing0',
      },
    );

    expect(stub).to.have.been.calledOnce;
    expect(stub).to.have.been.calledWith({
      pk: '00000000-0000-0000-0000-000000000000',
      sk: 'thing',
    }, {
      deleted: null,
      discriminator: 'thing',
      lastModifiedBy: 'system',
      latched: null,
      name: 'thing0',
      timestamp: 1600349040394,
      ttl: 1603200240,
    });
    expect(data).to.deep.equal({});
  });

  it('should get by id', async () => {
    const stub = sinon.stub(Connector.prototype, 'get')
      .resolves([{
        id: '00000000-0000-0000-0000-000000000000',
        name: 'thing0',
        timestamp: 1600144863435,
      }]);

    const connector = new Connector(
      debug('db'),
      't1',
    );

    const data = await get(
      { connector },
      '00000000-0000-0000-0000-000000000000',
    );

    expect(stub).to.have.been.calledOnce;
    expect(stub).to.have.been.calledWith('00000000-0000-0000-0000-000000000000');
    expect(data).to.deep.equal([{
      id: '00000000-0000-0000-0000-000000000000',
      name: 'thing0',
      timestamp: 1600144863435,
    }]);
  });

  it('should delete', async () => {
    sinon.stub(utils, 'now').returns(1600349040394);
    const stub = sinon.stub(Connector.prototype, 'update')
      .resolves({});

    const connector = new Connector(
      debug('db'),
      't1',
    );

    const data = await del(
      { connector },
      '00000000-0000-0000-0000-000000000000',
    );

    expect(stub).to.have.been.calledOnce;
    expect(stub).to.have.been.calledWith({
      pk: '00000000-0000-0000-0000-000000000000',
      sk: 'thing',
    }, {
      deleted: true,
      discriminator: 'thing',
      lastModifiedBy: 'system',
      latched: null,
      timestamp: 1600349040394,
      ttl: 1601299440,
    });
    expect(data).to.deep.equal({});
  });

  it('should map to update request', () => {
    const uow = {
      event: {
        type: 'thing-deleted',
        thing: {
          id: '00000000-0000-0000-0000-000000000000',
          name: 'thing0',
        },
      },
    };

    // console.log(toUpdateRequest(uow));

    expect(toUpdateRequest(uow)).to.deep.equal({
      Key: { pk: '00000000-0000-0000-0000-000000000000', sk: 'thing' },
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
        '#discriminator': 'discriminator',
        '#lastModifiedBy': 'lastModifiedBy',
        '#timestamp': 'timestamp',
        '#deleted': 'deleted',
        '#latched': 'latched',
        '#ttl': 'ttl',
      },
      ExpressionAttributeValues: {
        ':id': '00000000-0000-0000-0000-000000000000',
        ':name': 'thing0',
        ':discriminator': 'thing',
        ':lastModifiedBy': 'system',
        ':timestamp': undefined,
        ':deleted': true,
        ':latched': true,
        ':ttl': NaN,
      },
      UpdateExpression: 'SET #id = :id, #name = :name, #discriminator = :discriminator, #lastModifiedBy = :lastModifiedBy, #timestamp = :timestamp, #deleted = :deleted, #latched = :latched, #ttl = :ttl',
      ReturnValues: 'ALL_NEW',
      ConditionExpression: 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
    });
  });

  it('should map to event', () => {
    const uow = {
      event: {
        raw: {
          new: {
            pk: '00000000-0000-0000-0000-000000000000',
            sk: 'Thing',
            discriminator: 'Thing',
            name: 'thing0',
            ttl: 1551818222,
          },
        },
      },
    };

    expect(toEvent(uow)).to.deep.equal({
      thing: {
        id: '00000000-0000-0000-0000-000000000000',
        name: 'thing0',
      },
      raw: undefined, // removing data from the default event
    });
  });
});
