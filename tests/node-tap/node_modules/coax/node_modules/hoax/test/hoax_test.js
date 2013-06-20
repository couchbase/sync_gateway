var hoax = require('../lib/hoax.js'),
  request = require("request");

var http = require("http"), url = require("url");

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
      var body = "";
      req.on("data", function(data) {body = body + data;});
      req.on("end", function(){
        res.statusCode = 200;
        res.end(JSON.stringify({url:req.url,awesome:true, method : req.method, body:body}));
      });
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
    hoax("http://localhost:3001/awesome", function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.done();
    });
  },
  '200 array' : function(test) {
    // test.expect()
    hoax(["http://localhost:3001/","very","awesome"], function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'GET', 'should be get.');
      test.done();
    });
  },
  '200 put JSON' : function(test) {
    // test.expect()
    hoax.put("http://localhost:3001/very/awesome", {my:"data"}, function(err, json){
      test.equal(err, null);
      test.equal(json.body, '{"my":"data"}', 'should be json.');
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  '200 array put' : function(test) {
    // test.expect()
    hoax.put(["http://localhost:3001/","very","awesome"], {my:"data"}, function(err, json){
      test.equal(err, null);
      test.equal(json.body, '{"my":"data"}', 'should be json.');
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  '200 post' : function(test) {
    test.expect(3);
    hoax.post("http://localhost:3001/very/awesome", function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'POST', 'should be put.');
      test.done();
    });
  },
  '200 array curry' : function(test) {
    // test.expect()
    var host = hoax("http://localhost:3001/"),
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
    var host = hoax("http://localhost:3001/"),
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
    var host = hoax("http://localhost:3001/"),
      resource = host(["very","awesome"]);
    resource.put(function(err, json){
      test.equal(err, null);
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  },
  '200 array curry post' : function(test) {
    // test.expect()
    var host = hoax("http://localhost:3001/"),
      resource = host(["very","awesome"]);
    resource.post("", {myjson:"safe"}, function(err, json){
      test.equal(err, null);
      test.equal(json.body, '{"myjson":"safe"}', 'should be json.');
      test.equal(json.awesome, true, 'should be awesome.');
      test.equal(json.method, 'POST', 'should be put.');
      test.done();
    });
  },
  'put curry' : function(test) {
    // test.expect()
    var host = hoax("http://localhost:3001/"),
      resource = host.put(["very","awesome"]);
    resource("coat",function(err, json){
      test.equal(err, null);
      test.equal(json.coat, true, 'should be awesome.');
      test.equal(json.method, 'PUT', 'should be put.');
      test.done();
    });
  }
};

var query = hoax("http://localhost:3001/very/awesome");
exports['/query'] = {
  setUp: function(done) {
    // setup here
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
      test.deepEqual(json.url, "/very/awesome?myquery=exciting", 'should be "exciting".');
      test.done();
    });
  },
  'put body': function(test) {
    // test.expect(2);
    // tests here
    query.put({myquery:"exciting"}, function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      // test.ok(json.query.myquery, 'should have query');
      test.deepEqual(json.url, "/very/awesome", 'should be boring.');
      test.deepEqual(json.body, '{"myquery":"exciting"}', 'should be in the body.');
      test.done();
    });
  },
  'put array query w/ body': function(test) {
    // test.expect(2);
    // tests here
    query.put([{myquery:"exciting"}], {"lo":"el"}, function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      test.deepEqual(json.body, '{"lo":"el"}', 'should be in the body.');
      test.deepEqual(json.url, "/very/awesome?myquery=exciting", 'should be "exciting".');
      test.done();
    });
  },
  'put query w/ body': function(test) {
    // test.expect(2);
    // tests here
    query.put({myquery:"exciting"}, {"lo":"el"}, function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      test.deepEqual(json.body, '{"lo":"el"}', 'should be in the body.');
      test.deepEqual(json.url, "/very/awesome?myquery=exciting", 'should be "exciting".');
      test.done();
    });
  },
  'put query no body': function(test) {
    // test.expect(2);
    // tests here
    query.put([{myquery:"exciting"}], function(err, json){
      // console.log(ok.statusCode, body);
      test.equal(err, null);
      test.ok(json.url, 'should have query');
      // test.ok(json.query.myquery, 'should have query');
      test.deepEqual(json.url, "/very/awesome?myquery=exciting", 'should be "exciting".');
      test.done();
    });
  }
};

var base = hoax(["http://localhost:3001/very/awesome", {param : "yeah"}]);
exports['can curry params'] = {
  'merges right': function(test) {
    test.expect(1);
    base({more : "yep"}, function(err, ok) {
      test.equal(ok.url, '/very/awesome?param=yeah&more=yep');
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
    hoax("http://localhost:3001/error?status=404",
      function(errJSON, res){
      // console.log(ok.statusCode, body);
      test.equal(res.statusCode, 404, 'response is second argument on error');
      test.equal(errJSON.error, true, 'error should be body when parsed json and error code');
      test.done();
    });
  }
};

var db;
exports['extensions'] = {
  setUp: function(done) {
    // setup here
    handlers['/extensions'] = function(req, res) {
      var body = "";
      req.on("data", function(data) {body = body + data;});
      req.on("end", function(){
        res.statusCode = 200;
        res.end(JSON.stringify({url:req.url,awesome:true, method : req.method, body:body}));
      });
    };

    db = hoax(["http://localhost:3001/extensions", {myKey : "valuable"}]);
    db.extend('mymethod', function(){return 'ok';});
    db.extend('usethis', function(cb){this([{ice:"cream"}],cb);});
    done();
  },
  'is callable': function(test) {
    test.expect(1);
    // tests here
    test.equal(db.mymethod(), 'ok');
    test.done();
  },
  'has working this': function(test) {
    test.expect(1);
    // tests here
    db.usethis(function(err, ok){
      test.equal(ok.url, '/extensions?myKey=valuable&ice=cream');
      test.done();
    });
  },
  'is inherited': function(test) {
    test.expect(1);
    // tests here
    var doc = db("deeper");
    test.equal(doc.mymethod(), 'ok');
    test.done();
  }
};


