import sinon from 'sinon';
import chai from 'chai';
import sinonChai from 'sinon-chai';
import Promise from 'bluebird';

chai.use(sinonChai);

sinon.usingPromise(Promise);
