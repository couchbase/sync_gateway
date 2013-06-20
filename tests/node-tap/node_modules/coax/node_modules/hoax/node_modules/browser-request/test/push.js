#!/usr/bin/env node
//
// Push all the test code to CouchDB.

var SP = require('static-plus')

if(require.main === module)
  main()

function main() {
  var DB = process.argv[2];
  if(!DB)
    return console.error('Usage: build.js <database_url>')

  var builder = new SP.Builder
  builder.target = DB + '/tests'

  // The landing page and (generated) Javascript is static attachments.
  builder.load('', __dirname + '/index.html', 'text/html')

  builder.load('browser/request.js'         , __dirname + '/../dist/browser/request.js'         , 'application/javascript')
  builder.load('browser-min/request.js'     , __dirname + '/../dist/browser/request-min.js'     , 'application/javascript')
  builder.load('ender/request.js'           , __dirname + '/../dist/ender/ender.js'             , 'application/javascript')
  builder.load('requirejs/xmlhttprequest.js', __dirname + '/../dist/requirejs/xmlhttprequest.js', 'application/javascript')
  builder.load('requirejs/request.js'       , __dirname + '/../dist/requirejs/request.js'       , 'application/javascript')
  builder.load('requirejs/require.js'       , __dirname + '/requirejs/require.js'               , 'application/javascript')
  builder.load('requirejs/test.js'          , __dirname + '/requirejs/test.js'                  , 'application/javascript')
  builder.load('browserify/test.js'         , __dirname + '/browserify/browserified.js'         , 'application/javascript')

  // Each test is a web page, made from a common template.
  builder.template = __dirname + '/page.tmpl.html'

  builder.doc({_id:'browser'    , name:'Browser (full)'    , script:'request.js', start_me:true})
  builder.doc({_id:'browser-min', name:'Browser (minified)', script:'request.js', start_me:true})
  builder.doc({_id:'requirejs'  , name:'Requirejs'         , script:'require.js'})
  builder.doc({_id:'browserify' , name:'Browserify'        , script:'test.js'   })
  builder.doc({_id:'ender'      , name:'Ender'             , script:'request.js'})

  builder.deploy()
  builder.on('deploy', function(result) {
    console.log('Deployed: %s/%s/', result.url, builder.namespace)
  })
}
