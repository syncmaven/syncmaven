const path = require("path");
const webpack = require("webpack");

const config = {
  entry: "./src/index.ts",
  target: "node",
  externals: {
    "better-sqlite3": "require('better-sqlite3')",
    express: "require('express')",
    nunjucks: "require('nunjucks')",
  },
  node: {
    __dirname: false,
  },
  devtool: "source-map",
  output: {
    path: path.resolve(__dirname, "dist"),
  },
  plugins: [
    new webpack.IgnorePlugin({ resourceRegExp: /^fsevents$/ }), // Ignore MacOS-only module
    new webpack.IgnorePlugin({ resourceRegExp: /^pg-native$/ }), // Ignore MacOS-only module
  ],
  module: {
    rules: [
      {
        test: /\.(ts)$/i,
        use: {
          loader: "babel-loader",
        },
      },
      {
        test: /\.node$/,
        loader: "node-loader",
      },
      // { test: /\.json$/, loader: "json-loader" },
    ],
  },
  optimization: {
    minimize: false,
  },
  resolve: {
    extensions: [".ts", ".js", ".node", "..."],
  },
  mode: "production",
};

module.exports = () => config;
