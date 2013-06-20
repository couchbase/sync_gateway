# pax

Curry your path segments with intelligent escaping. Tiny. Used by `hoax` and `coax`, hopefully your library too.

[![Build Status](https://travis-ci.org/jchris/pax.png?branch=master)](https://travis-ci.org/jchris/pax)

## Getting Started
Install the module with: `npm install pax`

It likes strings:

```javascript
var pax = require('pax'),
	site = pax("http://www.couchbase.com",{myDefault : "query"}),
	jchris = site("jchris");

jchris.toString() === "http://www.couchbase.com/jchris?myDefault=query"
```

And arrays:

```javascript
var pax = require('pax'),
	site = pax(["http://www.couchbase.com","people",{myDefault : "query"}]),
	jchris = site(["jchris","party"]);

jchris.toString() === "http://www.couchbase.com/jchris/party?myDefault=query"
```

Lots of tests (run with `grunt`)

## License
Copyright (c) 2013 Chris Anderson
Licensed under the APL license.
