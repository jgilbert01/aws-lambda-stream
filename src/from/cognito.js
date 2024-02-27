/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import _ from 'highland';
import kebabCase from 'lodash/kebabCase';

import { faulty, uuid } from '../utils';

export const fromCognito = (event, { eventTypePrefix = 'aws-cognito' } = {}) => // eslint-disable-line import/prefer-default-export

  // event: https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-identity-pools-working-with-aws-lambda-triggers.html#cognito-user-pools-lambda-trigger-event-parameter-shared
  _([{
    // create a unit-of-work for the single event
    // create a stream to work with the rest of the framework
    record: event,
  }])
    .map(faulty((uow) =>
      // create a unit-of-work for each event
      // so we can correlate related work for error handling
      ({
        ...uow,
        event: {
          id: uuid.v4(),
          type: `${eventTypePrefix}-${kebabCase(uow.record.triggerSource)}`,
          partitionKey: uow.record.userName,
          tags: {
            region: uow.record.region,
            source: uow.record.userPoolId,
          },
          raw: {
            ...uow.record.request,
          },
        },
      })));
