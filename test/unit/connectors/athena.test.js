import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { mockClient } from 'aws-sdk-client-mock';
import { AthenaClient, StartQueryExecutionCommand } from '@aws-sdk/client-athena';

import debug from 'debug';
import Connector from '../../../src/connectors/athena';

describe('connectors/athena.js', () => {
  let mockAthena;

  beforeEach(() => {
    mockAthena = mockClient(AthenaClient);
  });

  afterEach(() => {
    mockAthena.restore();
  });

  it('should reuse client per pipeline', () => {
    const client1 = Connector.getClient('test1', debug('test'));
    const client2 = Connector.getClient('test1', debug('test'));
    const client3 = Connector.getClient('test2', debug('test'));

    expect(client1).to.eq(client2);
    expect(client2).to.not.eq(client3);
  });

  it('should start query exec', async () => {
    const spy = sinon.spy((_) => ({}));
    mockAthena.on(StartQueryExecutionCommand).callsFake(spy);

    const inputParams = {
      QueryString: 'select * from my_table',
    };

    const data = await new Connector({
      debug: debug('athena'),
    }).startQueryExecution(inputParams);

    expect(spy).to.have.been.calledWith({
      QueryString: 'select * from my_table',
    });
    expect(data).to.deep.equal({});
  });
});
