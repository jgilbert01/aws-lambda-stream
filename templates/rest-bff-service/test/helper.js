import sinon from 'sinon';
import chai from 'chai';
import sinonChai from 'sinon-chai';
import Promise from 'bluebird';
import AWS from 'aws-sdk-mock';

chai.use(sinonChai);

sinon.usingPromise(Promise);
AWS.Promise = Promise;
