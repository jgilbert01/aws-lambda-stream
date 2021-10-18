import 'mocha';
import { expect } from 'chai';
import * as sinon from 'sinon';

import Model from '../../../../src/models/thing';
import { getThing, saveThing, deleteThing } from '../../../../src/rest/routes/thing';

class Response {
  constructor() {
    this.status = sinon.stub().returns(this);
    this.json = sinon.spy((data) => data);
  }
}

describe('rest/routes/thing.js', () => {
  afterEach(sinon.restore);

  it('should save', async () => {
    const stub = sinon.stub(Model.prototype, 'save').resolves({});

    const request = {
      namespace: {
        models: {
          thing: new Model(),
        },
      },
      params: {
        id: '00000000-0000-0000-0000-000000000000',
      },
      body: {
        name: 'thing0',
      },
    };
    const response = new Response();

    const data = await saveThing(request, response);

    expect(stub).to.have.been.calledWith('00000000-0000-0000-0000-000000000000', { name: 'thing0' });

    expect(response.status).to.have.been.calledWith(200);
    expect(response.json).to.have.been.calledWith({});
    expect(data).to.deep.equal({});
  });

  it('should get by id', async () => {
    const stub = sinon.stub(Model.prototype, 'get').resolves({
      id: '00000000-0000-0000-0000-000000000000',
      name: 'thing0',
    });

    const request = {
      namespace: {
        models: {
          thing: new Model(),
        },
      },
      params: {
        id: '00000000-0000-0000-0000-000000000000',
      },
    };
    const response = new Response();

    const data = await getThing(request, response);

    expect(stub).to.have.been.calledWith('00000000-0000-0000-0000-000000000000');

    expect(response.status).to.have.been.calledWith(200);
    expect(response.json).to.have.been.calledWith({
      id: '00000000-0000-0000-0000-000000000000',
      name: 'thing0',
    });
    expect(data).to.deep.equal({
      id: '00000000-0000-0000-0000-000000000000',
      name: 'thing0',
    });
  });

  it('should delete', async () => {
    const stub = sinon.stub(Model.prototype, 'delete').resolves({});

    const request = {
      namespace: {
        models: {
          thing: new Model(),
        },
      },
      params: {
        id: '00000000-0000-0000-0000-000000000000',
      },
    };
    const response = new Response();

    const data = await deleteThing(request, response);

    expect(stub).to.have.been.calledWith('00000000-0000-0000-0000-000000000000');

    expect(response.status).to.have.been.calledWith(200);
    expect(response.json).to.have.been.calledWith({});
    expect(data).to.deep.equal({});
  });
});
