'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');

class Plugin {
  constructor(serverless, options) {
    this.hooks = {
      'after:deploy:deploy': afterDeploy.bind(null, serverless, options)
    };
  }
}

module.exports = Plugin;

const afterDeploy = async (serverless, options) => {
  const es = client(host(serverless), options);
  const { retentionDays, forcePolicyUpdate } = serverless.service.custom.elasticsearch;
  await upsertPolicy(es, retentionDays, forcePolicyUpdate);
  await putTemplate(es);
};

const upsertPolicy = async (es, retentionDays, forcePolicyUpdate) => {
  const policy = await getPolicy(es).catch(() => ({}));
  const putPolicyOptions = policy ? { if_seq_no: policy._seq_no, if_primary_term: policy._primary_term } : undefined;
  const policyExists = putPolicyOptions.if_seq_no !== undefined || putPolicyOptions.if_primary_term;
  if ((forcePolicyUpdate && policyExists) || !policyExists) {
    return putPolicy(es, retentionDays, putPolicyOptions);
  }
};

const getPolicy = async (es) => {
  return es.transport.request({
    method: 'GET',
    path: '/_opendistro/_ism/policies/event-policy',
  });
};

const putPolicy = (es, retentionDays, querystring = {}) => {
  return es.transport.request({
    method: 'PUT',
    path: '/_opendistro/_ism/policies/event-policy',
    querystring,
    body: {
      policy: {
        "description": "events policy",
        "default_state": "hot",
        "states": [
          {
            "name": "hot",
            "actions": [],
            "transitions": [
              {
                "state_name": "delete",
                "conditions": {
                  "min_index_age": `${retentionDays}d`
                }
              }
            ]
          },
          {
            "name": "delete",
            "actions": [
              {
                "delete": {}
              }
            ]
          }
        ],
        "ism_template": {
          "index_patterns": ["events-*"],
          "priority": 100
        }
      }
    },
  })
};

const eventProperties = {
  id: {
    type: 'keyword',
    index: true,
  },
  type: {
    type: 'keyword',
    index: true,
  },
  partitionKey: {
    type: 'keyword',
    index: true,
  },
  timestamp: {
    type: 'date',
  },
  tags: {
    dynamic: true,
    properties: {},
  },
};

const putTemplate = (es) => es.indices.putTemplate(
  {
    name: 'event',
    body: {
      index_patterns: ['events-*'],
      aliases: {
        events: {}
      },
      settings: {
        'index.refresh_interval': "30s",
      },
      mappings: {
        dynamic: false,
        properties: {
          time: {
            type: 'date'
          },
          detail: {
            dynamic: false,
            properties: {
              ...eventProperties,
              err: {
                dynamic: false,
                properties: {
                  name: {
                    type: 'keyword',
                  },
                  message: {
                    type: 'text',
                  },
                },
              },
              // uow shows up in fault events, so index the event details in there
              uow: {
                dynamic: false,
                properties: {
                  ...eventProperties,
                }
              }
            }
          }
        }          
      }
    }
  });

const host = (serverless) => {
  const awsInfo = _.find(serverless.pluginManager.getPlugins(), (plugin) => {
    return plugin.constructor.name === 'AwsInfo';
  });

  if (!awsInfo || !awsInfo.gatheredData) {
    return;
  }

  const outputs = awsInfo.gatheredData.outputs;

  const domainEndpoint = _.find(outputs, (output) => {
    return output.OutputKey === 'DomainEndpoint';
  });

  if (!domainEndpoint || !domainEndpoint.OutputValue) {
    return;
  }

  return domainEndpoint.OutputValue;
};

const client = (host, options) => {
  AWS.config.region = options.region;

  return require('elasticsearch').Client({
    hosts: [`https://${host}`],
    connectionClass: require('http-aws-es'),
    log: 'trace',
  });
}
