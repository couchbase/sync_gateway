var  request = require("request")
    , qs = require('querystring');
// coux is a tiny couch client, there are implementations for server side and client side
// this implementation is node.js

var coux = function(opts, body) {
    if (typeof opts === 'string' || Array.isArray(opts)) {
        opts = {url:opts};
    }
    var cb = arguments[Math.max(1,arguments.length -1)] || function() {console.log("empty callback", opts)};

    if (arguments.length == 3) {
        opts.body = JSON.stringify(body);
    } else {
        body = null;
    }
    opts.url = opts.url || opts.uri;
    if (Array.isArray(opts.url)) {
        var first = true;
        if (typeof opts.url[opts.url.length-1] == 'object') {
          var q, qObj = opts.url.pop();
          for (q in qObj) {
            if (["key", "startkey", "endkey", "start_key", "end_key"].indexOf(q) != -1) {
              qObj[q] = JSON.stringify(qObj[q]);
            }
          }
            var query = qs.stringify(qObj);
        }
        opts.url = (opts.url.map(function(path) {
            if (first) {
                first = false;
                if (/^http/.test(path)) {
                  if (/\/$/.test(path)) {
                    return path.substring(0,path.length-1)
                  } else {
                    return path;                    
                  }
                }
            }
            return encodeURIComponent(path);
        })).join('/');
        if (query) {
            opts.url = opts.url + "?" + query;
        }
    }
    var req = {
        method: 'GET',
        headers : {
            'content-type': 'application/json'
        }
    };
    for (var x in opts) {
        if (opts.hasOwnProperty(x)){
            req[x] = opts[x];
        }
    }
    log('coux', req.method, req.url, body);
    return request(req, function(err, resp, body) {
        var jsonBody;
        log('done', req.method, req.url);
        if (err) {
            cb(err, resp, body)
        } else {
            try {
                jsonBody = JSON.parse(body);
            } catch(e) {
                jsonBody = body;
            }
            if (resp.statusCode >= 400) {
                cb(jsonBody, resp)
            } else {
                cb(false, jsonBody)
            }
        }
    });
};

module.exports = coux;
coux.coux = coux;

coux.put = function() {
    var opts = arguments[0];
    if (typeof opts === 'string' || Array.isArray(opts)) {
        opts = {url:opts};
    }
    opts.method = "PUT";
    arguments[0] = opts;
    coux.apply(null, arguments);
};

coux.post = function() {
    var opts = arguments[0];
    if (typeof opts === 'string' || Array.isArray(opts)) {
        opts = {url:opts};
    }
    opts.method = "POST";
    arguments[0] = opts;
    coux.apply(null, arguments);
};

coux.del = function() {
    var opts = arguments[0];
    if (typeof opts === 'string' || Array.isArray(opts)) {
        opts = {url:opts};
    }
    opts.method = "DELETE";
    arguments[0] = opts;
    coux.apply(null, arguments);
};


// connect to changes feed
coux.subscribeDb = function(db, fun, since) {
    var req, go = true;
    if (typeof db == "string") {
      db = [db];
    }
    function getChanges(since) {
      var opts = {
        url : db.concat("_changes", {include_docs:true,feed:"longpoll",since:since}), 
        agent : false
      };
      req = coux(opts, function(err, changes) {
        if (go) {
          if (err) {
            var wait = 5e3;
            console.error("coux changes", err)
            console.error("opts", opts)
            console.error("coux retry in", wait)
            setTimeout(function() {
              getChanges(since);
            }, wait);
          } else {
            if (changes.results) {
              changes.results.forEach(fun)              
            }
            getChanges(changes.last_seq || since);
          }
        }
      })
    }
    getChanges(since || 0);
    return {
        stop : function() {
            // console.log('stopped', req)
            go = false;
        }
    }
};

coux.waitForDoc = function(path, id, seq, cb) {
  if (typeof path == "string") {
    path = [path];
  }
  path = path.concat("_changes",{filter:"_doc_ids", since:(seq||0), 
    include_docs:true, feed:"longpoll"});
  coux.post({
    url : path, agent : false
  }, {"doc_ids": [id]}, function(err, resp) {
    var row, r;
    while (row = resp.results.shift()) {
      r = cb(err, row.doc);
      if (!r) {
        break;
      }
    }
    if (r) {
      coux.waitForDoc(path, id, resp.last_seq, cb)
    }
  })
}


// dont log GET
// coux.log = ["PUT", "POST", "DELETE"];
coux.log = [];
// coux.log = ["PUT", "POST", "DELETE", "GET"];
function log(message, verb) {
    if (coux.log.indexOf(verb) !== -1) {
        console.log.apply(null, arguments);
    }
};