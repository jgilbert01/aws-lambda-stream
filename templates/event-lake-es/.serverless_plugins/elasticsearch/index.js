'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');

class Plugin {
  constructor(serverless, options) {
    this.hooks = {
      'after:deploy:deploy': putTemplate.bind(null, serverless, options)
    };
  }
}

module.exports = Plugin;

const putTemplate = (serverless, options) => {
  const es = client(host(serverless), options);

  return es.indices.putTemplate(
    {
      name: 'event',
      body: {
        index_patterns: 'events*', // 6.x
        aliases: {
          "events-all": {}
        },
        mappings: {
          _default_: {
            dynamic_templates: [{
              strings: {
                mapping: {
                  index: true,
                  type: 'keyword'
                },
                match: '*',
                match_mapping_type: 'string'
              }
            }]
          },
          event: {
            properties: {
              type: {
                type: 'keyword',
                index: true
              },
              timestamp: {
                type: 'date'
              },
              tags: {
                type: 'object',
              },
            }
          }
        }
      }
    }
  );
};

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
