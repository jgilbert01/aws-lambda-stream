import _ from 'highland';

export const fromCron = (event) => _([{ // eslint-disable-line import/prefer-default-export
  // create a unit-of-work for the single event
  // create a stream to work with the rest of the framework
  record: event,
  event,
}]);
