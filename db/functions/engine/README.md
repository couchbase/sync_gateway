# The Sync Gateway GraphQL/Functions Engine

This is a node.js project, although the actual runtime environment isn't node.js, rather just an in-process V8 runtime.

The TypeScript source code lives in `src`. This is compiled (transpiled) into JavaScript and merged into a single file, `dist/main.js`. Finally, the Go compiler embeds the contents of `main.js` into `environment.go` as the string constant `kJavaScriptCode`, which at runtime is passed to V8 to evaluate when it starts up.

Since most SG developers won't be changing the TypeScript code, the build output (`main.js`) is checked in. That means **you can build SG normally without having to install node.js or worry about any of this stuff.**

But if you _do_ touch anything herein...

## How To Rebuild After Changing TypeScript Files Or Updating JS Dependencies

### Prerequisites

Only needs to be done once per machine.

1. Install node.js (e.g. `brew install node`)
2. Install TypeScript (e.g. `brew install typescript`)

### Setup

This must be done before a clean build, and after any changes to package.json, i.e. after a git pull. It downloads or updates all of the module dependencies.

1. `cd db/functions/engine`
2. `npm install`

### Building It

On a clean build, or after any changes in the `engine` source tree:

1. `cd db/functions/engine`
2. `npm run build`

(Alternatively, if you're actively working on the TypeScript source files, use `npm run watch` instead: it will rebuild incrementally whenever you save changes to a file.)

The build product is the file `dist/main.js`. Now the next time you build SG itself, it will pick up the updated JavaScript. And you'll need to check the updated `main.js` into Git.
