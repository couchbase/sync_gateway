# The Sync Gateway GraphQL/Functions Engine

The source code is structured as a node.js project, although the actual runtime environment isn't node.js, rather just an in-process V8 runtime.

## Building It

1. Install node.js
2. `cd db/functions/engine`
3. `npm install`
4. `npm run build`

The build product is the file `dist/main.js`. During the Go build, the contents of this file are compiled into `environment.go` as the string constant `kJavaScriptCode`, which is then passed to V8 to evaluate when it starts up.
