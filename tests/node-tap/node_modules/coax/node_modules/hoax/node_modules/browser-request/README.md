# Browser Request: The easiest HTTP library you'll ever see

Browser Request is a port of Mikeal Rogers's ubiquitous and excellent [request][req] package to the browser.

Jealous of Node.js? Pining for clever callbacks? Request is for you.

Don't care about Node.js? Looking for less tedium and a no-nonsense API? Request is for you too.

# Examples

Fetch a resource:

```javascript
request('/some/resource.txt', function(er, response, body) {
  if(er)
    throw er;
  console.log("I got: " + body);
})
```

Send a resource:

```javascript
request.put({uri:'/some/resource.xml', body:'<foo><bar/></foo>'}, function(er, response) {
  if(er)
    throw new Error("XML PUT failed (" + er + "): HTTP status was " + response.status);
  console.log("Stored the XML");
})
```

To work with JSON, set `options.json` to `true`. Request will set the `Content-Type` and `Accept` headers, and handle parsing and serialization.

```javascript
request({method:'POST', url:'/db', body:'{"relaxed":true}', json:true}, on_response)

function on_response(er, response, body) {
  if(er)
    throw er
  if(result.ok)
    console.log('Server ok, id = ' + result.id)
}
```

Or, use this shorthand version (pass data into the `json` option directly):

```javascript
request({method:'POST', url:'/db', json:{relaxed:true}}, on_response)
```

## Convenient CouchDB

Browser Request provides a CouchDB wrapper. It is the same as the JSON wrapper, however it will indicate an error if the HTTP query was fine, but there was a problem at the database level. The most common example is `409 Conflict`.

```javascript
request.couch({method:'PUT', url:'/db/existing_doc', body:{"will_conflict":"you bet!"}}, function(er, resp, result) {
  if(er.error === 'conflict')
    return console.error("Couch said no: " + er.reason); // Output: Couch said no: Document update conflict.

  if(er)
    throw er;

  console.log("Existing doc stored. This must have been the first run.");
})
```

See the [Node.js Request README][req] for several more examples. Request intends to maintain feature parity with Node request (except what the browser disallows). If you find a discrepancy, please submit a bug report. Thanks!

# Usage

## Browserify

Browser Request is a [browserify][browserify]-enabled package.

First, add `browser-request` to your Node project

    $ npm install browser-request

Next, make a module that uses the package.

```javascript
// example.js - Example front-end (client-side) code using browser-request via browserify
//
var request = require('browser-request')
request('/', function(er, res) {
  if(!er)
    return console.log('browser-request got your root path:\n' + res.body)

  console.log('There was an error, but at least browser-request loaded and ran!')
  throw er
})
```

To build this for the browser, run it throubh browserify.

    $ browserify --entry example.js --outfile example-built.js

Deploy `example-built.js` to your web site and use it from your page.

```html
  <script src="example-built.js"></script> <!-- Runs the request, outputs the result to the console -->
```

## Ender

Browser Request is an [Ender][ender] package. If you don't have Ender, install it, and don't ever look back.

    $ ender add browser-request

## RequireJS

Browser Request also supports [RequireJS][rjs]. Add `dist/requirejs/request.js` and `dist/requirejs/xmlhttprequest.js` to your web application and use it from your code.

```javascript
require(['request'], function(request) {
  // request is ready.
})
```

## Traditional

The traditional way is to use it like any other Javascript library. Add `dist/browser/request.js` to your web application and use it from your page.

```html
<script src="request.js"></script>
<script>
    request("/motd.html", function(er, res) {
        if(er)
            return console.error('Failed to get the message of the day')
        console.log('Got the message of the day')
    })
</script>
```

## License

Browser Request is licensed under the Apache 2.0 license.

Browser Request uses Sergey Ilinsky's [XMLHttpRequest][xhr] package, licended under the terms of the LGPL 2.1 or later.

[req]: https://github.com/mikeal/request
[rjs]: http://requirejs.org/
[xhr]: https://github.com/ilinsky/xmlhttprequest
[ender]: http://ender.no.de
