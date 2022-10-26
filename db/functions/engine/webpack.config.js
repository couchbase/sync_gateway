// Generated using webpack-cli https://github.com/webpack/webpack-cli

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
            // Omit this module entirely (any calls will throw):
            "fs":       false,
        }
    },

    devtool: 'inline-source-map',       // put JS source map in the code itself

    // optimization: {
    //     minimize: false,                // turn off size minimization; leave code readable
    // },

    ignoreWarnings: [                   // Suppress Web-specific warnings about big code
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
