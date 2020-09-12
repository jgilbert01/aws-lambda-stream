import { collect } from 'aws-lambda-stream';

const events = [
  'thing-submitted',
];

export default [
  {
    id: 'clt1',
    flavor: collect,
    eventType: events,
  },
];
