import { evaluate } from 'aws-lambda-stream';

export default [
  {
    id: 'eval1',
    flavor: evaluate,
    eventType: 'thing-submitted',
    // expression: (uow) => uow.correlated.find((e) => ['t1', 't2'].contains(e.type)).length === 2,
    emit: 'thing-xyz',
    // emit: (uow, rule, template) => ({
    //   ...template,
    //   type: 'thing-xyz',
    //   thing: uow.event.thing,
    // }),
  },
];
