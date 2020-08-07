const slsw = require('serverless-webpack');
const nodeExternals = require("webpack-node-externals");
const path = require('path');

const injectMocks = (entries) =>
  Object.keys(entries).reduce((e, key) => {
    e[key] = ['regenerator-runtime/runtime', './test/int/mocks.js', entries[key]];
    return e;
  }, {});

const includeMocks = () => slsw.lib.webpack.isLocal && process.env.REPLAY !== 'bloody';

const entry = includeMocks() ? injectMocks(slsw.lib.entries) : slsw.lib.entries;

module.exports = {
  entry: entry,
  output: {
    libraryTarget: 'commonjs',
    path: path.join(__dirname, '.webpack'),
    filename: '[name].js'
  },
  optimization: {
    minimize: false
  },
  target: 'node',
  mode: slsw.lib.webpack.isLocal ? "development" : "production",
  externals: [
    nodeExternals()
  ],
  module: {
    rules: [
      {
        test: /\.js$/,
        use: [
          {
            loader: 'babel-loader',
          }
        ],
        include: __dirname,
        exclude: /node_modules/
      }
    ]
  }
};
