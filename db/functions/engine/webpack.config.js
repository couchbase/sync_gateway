/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

const path = require('path');

const isProduction = process.env.NODE_ENV == 'production';


const config = {
    entry: './src/api.ts',                          // <-- main module

    output: {
        path: path.resolve(__dirname, 'dist'),      // <-- JS output directory
        library: 'SG_Engine',                       // <-- expose as global `SG_Engine` object
    },

    module: {
        rules: [
            {
                test: /\.(ts|tsx)$/i,               // Transpile TypeScript to JS
                loader: 'ts-loader',
                exclude: ['/node_modules/'],
            },
        ],
    },

    resolve: {
        extensions: ['.tsx', '.ts', '.jsx', '.js', '...'],

        fallback: {
            // @apollo modules call node.js APIs. Substitute browser-compatible alternatives:
            "assert":   require.resolve("assert/"),
            "console":  require.resolve("console-browserify"),
            "path":     require.resolve("path-browserify"),
            "url":      path.resolve(__dirname, 'polyfill/url.js'),
            "util":     require.resolve("util/"),
            // Omit this module entirely (any calls will throw) but it's never called:
            "fs":       false,
        }
    },

    devtool: 'inline-source-map',           // put JS source map in the code itself

    // optimization: {
    //     minimize: false,                 // turn off size minimization; leave code readable
    // },

    ignoreWarnings: [                       // Suppress Web-specific warnings about big code
        /asset size limit/, /entrypoint size limit/, /limit the size/
    ],
};

module.exports = () => {
    if (isProduction) {
        config.mode = 'production';
    } else {
        config.mode = 'development';
    }
    return config;
};
