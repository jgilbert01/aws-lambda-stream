const _ = require('highland');
const Promise = require('bluebird');
const aws = require('aws-sdk')

const {
  S3Connector, split, rejectWithFault,
  invokeLambda,
} = require('aws-lambda-stream');

aws.config.setPromisesDependency(Promise);

const _debug = require('debug')('cli');

const now = require('moment')().utc();

const replacer = (k, v) => {
  // console.log('IS BUFFER: ', k);
  // console.log('IS BUFFER: ', Buffer.isBuffer(v) || (v && v.type === 'Buffer'));
  // console.log('IS BUFFER: ', typeof v);
  // console.log('IS BUFFER: ', v);
  return (v && v.type === 'Buffer') ? `[BUFFER: ${v.data.length}]` : v;
};
const debug = data => _debug(JSON.stringify(data, replacer, 2));
const print = data => console.log(JSON.stringify(data, replacer, 2));

const findUp = require('find-up');
const fs = require('fs');

const configPath = findUp.sync(['.faultsrc', '.faultsrc.json'])
const argv = {
  ...(configPath ? JSON.parse(fs.readFileSync(configPath)) : {}),
  prefix: process.env.PREFIX || `${now.format('YYYY')}/${now.format('MM')}/${now.format('DD')}/`,
  functionname: process.env.FUNCTION_NAME,
  dry: process.env.DRY_RUN === 'true',
  continuationToken: process.env.MARKER,
  batch: 25,
  parallel: 16,
  batchTimeout: process.env.BATCH_TIMEOUT ? Number(process.env.BATCH_TIMEOUT) : 5000,
  rate: process.env.RATE ? Number(process.env.RATE) : 3,
  window: 500,
};

const start = Date.now();
const runtime = () => (Date.now() - start) / 1000 / 60;

const counters = {
  list: 0,
  get: 0,
  events: 0,
  match: 0,
  recordCount: 0,
};

const main = () => {
  print(argv);
  // print(process.env);

  head(argv)
    .filter(filterByFunctionname(argv))

    .filter(
      uow => (uow.event && uow.event.uow && uow.event.uow.record) ||
        (uow.event && uow.event.uow && uow.event.uow.batch && uow.event.uow.batch[0] &&
          uow.event.uow.batch[0].record)
    )

    .map((uow => {
        let payload = {
            Records: uow.event.uow.batch ?
                uow.event.uow.batch.map(b => b.record) :
                [uow.event.uow.record]
        };

        payload = Buffer.from(JSON.stringify(payload));

        return {
            ...uow,
            recordCount: uow.event.uow.batch ? uow.event.uow.batch.length : 1,
            invokeRequest: {
                FunctionName: uow.event.tags.functionname,
                Qualifier: argv.qualifier,
                InvocationType:
                    argv.dry ? 'DryRun' :
                        argv.async && payload.length <= 100000 ?
                            'Event' :
                            'RequestResponse',
                Payload: payload,
            },
        };
    }))
    // .tap(print)

    .ratelimit(argv.rate, argv.window)
    .through(invokeLambda({ parallel: argv.parallel }))
    .tap(debug)

    .errors(errors)

    .reduce(counters, count)

    .stopOnError(console.log)
    .done(() => {
      console.log('======================================');
      console.log('Running time (minutes): ', runtime());
      console.log('Gap: ', counters.list - counters.get);
      console.log('Final Counters:')
      print(counters);
      console.log('======================================');
    });
};

const filterByFunctionname = argv => (uow) => {
  if (argv.functionname === '*') {
    return true;
  } else {
    return argv.functionname === uow.event.tags.functionname;
  }
};

const count = (counters, uow) => {
  if (uow.event && uow.event.type) {
    const type = uow.event.type;
    const functionname = uow.event.tags.functionname;
    const pipeline = `${functionname}|${uow.event.tags.pipeline}`;

    counters.match = (counters.match ? counters.match : 0) + 1;

    if (!counters.types) counters.types = {};
    const types = counters.types;
    types[type] = (types[type] ? types[type] : 0) + 1;

    if (!counters.functions) counters.functions = {};
    const functions = counters.functions;
    functions[pipeline] = (functions[pipeline] ? functions[pipeline] : 0) + 1;
  }

  if (uow.recordCount) {
    counters.recordCount = (counters.recordCount ? counters.recordCount : 0) + uow.recordCount;
  }

  if (uow.invokeRequest) {
    if (!counters.invoked) counters.invoked = { total: 0, statuses: {} };
    counters.invoked.total = counters.invoked.total + 1;

    if (uow.invokeResponse) {
      const status = uow.invokeResponse.StatusCode;
      const statuses = counters.invoked.statuses;
      statuses[status] = (statuses[status] ? statuses[status] : 0) + 1;
    }
  }

  if (uow.err) {
    counters.errors = (counters.errors ? counters.errors : 0) + 1;
    if (!counters.errored) counters.errored = [];
    counters.errored.push(uow);
  }

  return counters;
};

const head = (argv) => {
  aws.config.region = argv.region;

  const uows = argv.prefix.split(',').map((prefix) => ({
    argv,
    listRequest: {
      Bucket: argv.bucket,
      // Delimiter: '/',
      Prefix: argv.region ?
        argv.region + '/' + prefix :
        prefix,
      ContinuationToken: !['', 'undefined', true]
        .includes(argv.continuationToken) ? argv.continuationToken : undefined,
    },
  }));

  return _(uows)
    .through(pageObjectsFromS3({ parallel: 1 }))
    .map((uow) => ({
      ...uow,
      getRequest: {
        Bucket: argv.bucket,
        Key: uow.listResponse.Content.Key,
      },
    }))
    .through(getObjectFromS3({ parallel: argv.parallel }))
    .flatMap(split())
    .map((uow) => {
      counters.events = (counters.events ? counters.events : 0) + 1;
      const { detail, ...eb } = JSON.parse(uow.getResponse.line);
      return ({
        ...uow,
        record: {
          ...uow.record,
          eb,
        },
        event: detail,
      });
    })
    .tap(debug);
};

const errors = (error, push) => {
  console.error(error.message);
  if (error.message === 'The provided token has expired.') {
    push(error);
  } else if (error.uow) {
    // catch so we can count and log at the end
    const { uow, ...err } = error;
    push(null, { ...uow, err });
  } else {
    push(error);
  }
}

const pageObjectsFromS3 = ({
  debug = require('debug')('s3'),
  bucketName = process.env.BUCKET_NAME,
  listRequestField = 'listRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 1,
} = {}) => {
  const connector = new S3Connector({ debug, bucketName });

  const listObjects = (uow) => {
    let ContinuationToken = uow[listRequestField].ContinuationToken;

    return _((push, next) => {
      const params = {
        ...uow[listRequestField],
        MaxKeys: Math.floor(runtime()) > 22 ? 2 : argv.parallel - 5, // slow down to catch up after 22 minutes
        ContinuationToken,
      };

      connector.listObjects(params)
        .then((data) => {
          const { Contents, ...rest } = data;

          if (rest.IsTruncated) {
            ContinuationToken = rest.NextContinuationToken;

          } else {
            ContinuationToken = undefined;
          }

          counters.list = (counters.list ? counters.list : 0) + Contents.length;

          console.log('======================================');
          console.log('Prefix: ', params.Prefix);
          console.log('ContinuationToken: ', ContinuationToken);
          console.log('Contents: ', Contents.length);
          console.log('Running time (minutes): ', runtime());
          console.log('Gap: ', counters.list - counters.get);
          console.log('Counters:');
          print(counters);
          console.log('======================================');


          Contents.forEach((obj) => {
            // console.log('List: ', obj.Key);

            push(null, {
              ...uow,
              [listRequestField]: params,
              listResponse: {
                ...rest,
                Content: obj,
              },
            });
          });
        })
        .catch(/* istanbul ignore next */(err) => {
          err.uow = uow;
          push(err, null);
        })
        .finally(() => {
          if (ContinuationToken) {
            next();
          } else {
            push(null, _.nil);
          }
        });
    });
  };

  return (s) => s
    .map(listObjects)
    .parallel(parallel);
};

const getObjectFromS3 = ({
  debug = require('debug')('s3'),
  bucketName = process.env.BUCKET_NAME,
  getRequestField = 'getRequest',
  parallel = Number(process.env.S3_PARALLEL) || Number(process.env.PARALLEL) || 8,
} = {}) => {
  const connector = new S3Connector({ debug, bucketName });

  const getObject = (uow) => {
    counters.get = (counters.get ? counters.get : 0) + 1;
    console.log('Get: ', uow[getRequestField].Key);
    const p = connector.getObject(uow[getRequestField])
      .then((getResponse) => ({ ...uow, getResponse }))
      .catch(rejectWithFault(uow));

    return _(p); // wrap promise in a stream
  };

  return (s) => s
    .map(getObject)
    .parallel(parallel);
};

main();
