# hoax

JSON HTTP client using [pax](https://npmjs.org/package/pax) for path currying and request for HTTP.

Used by [coax](https://npmjs.org/package/coax)

[![Build Status](https://travis-ci.org/jchris/hoax.png?branch=master)](https://travis-ci.org/jchris/hoax)

## Getting Started
Install the module with: `npm install hoax`

```javascript
var hoax = require('hoax');

hoax(["http://localhost:3001/","very","awesome"], function(err, json){
	console.log("fetched", json)
});

// currying
var server = hoax("http://localhost:3001/"),
	path = server("very", {query : "params"});

// put JSON to "http://localhost:3001/very/awesome?query=params"
path.put("awesome", {some:"data"}, function(err, json){
	console.log("put json", json);
});

```

## License
Copyright (c) 2013 Chris Anderson
Licensed under the Apache license.
