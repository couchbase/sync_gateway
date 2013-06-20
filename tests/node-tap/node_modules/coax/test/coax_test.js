var coax = require('../lib/coax.js'),
  request = require("request");

var http = require("http"), url = require("url"), qs = require("querystring");

var handlers = {};

var testServer = function() {
  http.createServer(function(req, res){
    // your custom error-handling logic:
    function error(status, err) {
      res.statusCode = status || 500;
      res.end(err.toString());
    }
    var path = url.parse(req.url).pathname;
    console.log("test server", req.method, req.url);
    if (handlers[path]) {
      handlers[path](req, res);
    } else {
      error(404, "no handler for "+path);
    }
  }).listen(3001);
};

testServer();

exports['/awesome'] = {
  setUp: function(done) {
    // setup here
    handlers['/awesome'] = function(req, res) {
      res.statusCode = 200;
      res.end(JSON.stringify({awesome:true}));
    };
    handlers['/very/awesome'] = function(req, res) {
      res.statusCode = 200;
      res.end(JSON.stringify({awesome:true, method : req.method}));
    };
    handlers['/very/awesome/coat'] = function(req, res) {
      res.statusCode = 200;
      res.end(JSON.stringify({coat:true, method : req.method}));
    };
    done();
  },
  '200 get': function(test) {
    test.expect(2);
    // tests here
    coax("http://localhost:3001/awesome", function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.done();
    });
  },
  '200 array' : function(test) {
    // test.expect()
    coax(["http://localhost:3001/","very","awesome"], function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'GET', 'should be get.');
      test.done();
    });
  },
  '200 put' : function(test) {
    // test.expect()
    coax.put("http://localhost:3001/very/awesome", function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  '200 array put' : function(test) {
    // test.expect()
    coax.put(["http://localhost:3001/","very","awesome"], function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  '200 post' : function(test) {
    test.expect(3);
    coax.post("http://localhost:3001/very/awesome", function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'POST', 'should be put.');
      test.done();
    });
  },
  '200 array curry' : function(test) {
    // test.expect()
    var host = coax("http://localhost:3001/"),
      resource = host(["very","awesome"]);
    resource("coat", function(err, json){
      test.equal(err, null);
      test.equal(json.coat, true, 'should be coat.');
      test.equal(json.method, 'GET', 'should be get.');
      test.done();
    });
  },
  '200 array no path' : function(test) {
    // test.expect()
    var host = coax("http://localhost:3001/"),
      resource = host(["very","awesome"]);
    resource(function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'GET', 'should be get.');
      test.done();
    });
  },
  '200 array curry put' : function(test) {
    // test.expect()
    var host = coax("http://localhost:3001/"),
      resource = host(["very","awesome"]);
    resource.put(function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  'put curry' : function(test) {
    // test.expect()
    var host = coax("http://localhost:3001/"),
      resource = host.put(["very","awesome"]);
    resource("coat",function(err, json){
      test.equal(err, null);
      test.equal(json.coat, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  }
};

var query = coax("http://localhost:3001/query");
exports['/query'] = {
  setUp: function(done) {
    // setup here
    handlers['/query'] = function(req, res) {
      res.statusCode = 200;
      res.end(JSON.stringify({url:req.url, method : req.method}));
    };
    done();
  },
  'get': function(test) {
    // test.expect(2);
    // tests here
    query({myquery:"exciting"}, function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      // test.ok(json.query.myquery, 'should have query');
      test.deepEqual(json.url, "/query?myquery=exciting", 'should be "exciting".');
      test.done();
    });
  },
  'get startkey etc': function(test) {
    // test.expect(2);
    // tests here

    query({startkey:[1,2], endkey:1, key:"ok",other:"ok"}, function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      // test.ok(json.query.myquery, 'should have query');
      test.deepEqual(json.url, "/query?startkey=%5B1%2C2%5D&endkey=1&key=%22ok%22&other=ok", 'should be "[1,2]".');
      test.done();
    });
  }
};

exports["/body"] = {
  setUp: function(done) {
    // setup here
    handlers['/body'] = function(req, res) {
      var body = "";
      req.on("data", function(data) {body = body + data;});
      req.on("end", function(){
        res.statusCode = 200;
        res.end(JSON.stringify({url:req.url, method : req.method, body:body}));
      });
    };
    done();
  },
  'post': function(test) {
    // test.expect(2);
    // tests here
    var db = coax("http://localhost:3001/body");
    db.post({'my':"doc"},
      function(errJSON, res){
      // console.log(ok.statusCode, body);
      test.equal(res.url, "/body", 'body');
      test.equal(res.method, "POST", 'post');
      test.equal(res.body, '{"my":"doc"}');
      test.done();
    });
  }
};

exports['/error'] = {
  setUp: function(done) {
    // setup here
    handlers['/error'] = function(req, res) {
      res.statusCode = 404;
      res.end(JSON.stringify({error:true}));
    };
    done();
  },
  '404': function(test) {
    // test.expect(2);
    // tests here
    coax("http://localhost:3001/error?status=404",
      function(errJSON, res){
      // console.log(ok.statusCode, body);
      test.equal(res.statusCode, 404, 'response is second argument on error');
      test.equal(errJSON.error, true, 'error should be body when parsed json and error code');
      test.done();
    });
  }
};

exports['_changes'] = {
  setUp: function(done) {
    // setup here
    handlers['/db/_changes'] = function(req, res) {
      res.statusCode = 200;
      var uri = qs.parse(url.parse(req.url).query);
      if (uri.since === "0") {
        res.end(JSON.stringify({last_seq:10, results:[1,2,3], url:req.url}));
      } else {
        res.end(JSON.stringify({last_seq:12, results:[4,5], url:req.url}));
      }
    };
    done();
  },
  'basics': function(test) {
    test.expect(5);
    // tests here
    var db = coax("http://localhost:3001/db");
    db.changes(function(err, change){
      test.ok(true, "change");
      if (change === 5) {
        test.done();
      }
    });
  }
};


