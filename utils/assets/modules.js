require=(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);throw new Error("Cannot find module '"+o+"'")}var f=n[o]={exports:{}};t[o][0].call(f.exports,function(e){var n=t[o][1][e];return s(n?n:e)},f,f.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({"f7r3xx":[function(require,module,exports){
/*
  SyncModel creates an in-memory representation of documents flowing
  through channels. It also mediates any server access so that UI
  components are abtracted from network interactions and the API is
  mostly synchronous queries from the UI triggered by SyncModel events.

  You MUST run ./modules.sh to see changes to this file in your app.
*/

var events = require('events'),
  coax = require("coax"),
  util = require("util");

var dbStateSingletons = {};
exports.SyncModelForDatabase = function(db) {
  var state = dbStateSingletons[db]
  if (!state) {
    state = new SyncModel(db)
    dbStateSingletons[db] = state
  }
  return state
}

function SyncModel(db) {
  // setup on / emit / etc
  events.EventEmitter.call(this);

  // private state
  var previewFun, self=this, client = coax(db),
    dbInfo = {}, previewChannels = {}, previewDocs = {};

  // public state
  this.db = db;
  this.client = client;
  this.pageSize = 100

  // pubic methods
  this.setSyncFunction = function(funCode) {
    var oldCode = previewFun && previewFun.code
    if (funCode == oldCode) {
      return;
    }
    previewChannels = {};
    previewDocs = {};
    previewFun = compileSyncFunction(funCode)
    previewFun.code = funCode
    loadChangesHistory()
    if (this.deployedSyncFunction() == funCode) {
      this.emit("syncReset", "deployed")
    } else {
      this.emit("syncReset", "preview")
    }
  }
  this.getSyncFunction = function() {
    return previewFun.code;
  }
  this.channelNames = function() {
    return Object.keys(previewChannels);
  }
  this.deployedSyncFunction = function(){
    return dbInfo.config.sync || "function(doc){\n  channel(doc.channels)\n}";
  }
  this.deploySyncFunction = function(code, done) {
    var newConfig = {}
    for (var k in dbInfo.config) {
      if (dbInfo.config[k]) {
        newConfig[k] = dbInfo.config[k]
      }
    }
    newConfig.sync = code;
    client.del([""]/*[""] to force trailing slash*/,function(err){
      if (err && err.constructor !== SyntaxError) {
        return done(err);
      }
      client.put([""]/*[""] to force trailing slash*/,newConfig, function(err, ok){
        if (err && err.constructor !== SyntaxError) {
          return done(err);
        }
        self.setSyncFunction(code)
        done(false, ok)
      })
    })
  }
  this.channel = function(name) {
    var changes = [], revs ={}, chan = previewChannels[name];
    if (!chan) return {name:name, changes:[]};
    var docs = chan.docs;

    for (var id in docs) revs[docs[id]] = id
    var rs = Object.keys(revs).sort(function(a, b){
      return parseInt(a) - parseInt(b);
    })
    for (var i = rs.length - 1; i >= 0; i--) {
      var docid = revs[rs[i]]
      changes.push({id:docid, seq:parseInt(rs[i]), isAccess : chan.access[docid]})
    }
    var result = {
      name : name,
      changes : changes
    }
    var accessIds = Object.keys(chan.access);
    if (accessIds.length) {
      result.access = chan.access
      result.hiddenAccessIds = [];
      for (i = accessIds.length - 1; i >= 0; i--) {
        if (!docs[accessIds[i]]) {
          result.hiddenAccessIds.push(accessIds[i])
        }
      }
    }
    return result
  }
  this.randomAccessDocID = function() {
    var chs = this.channelNames()
    chs = shuffleArray(chs);
    var ch = chs.pop();
    while (ch) {
      var chInfo = this.channel(ch)
      if (chInfo.access) {
        var ids = Object.keys(chInfo.access)
        return ids[Math.floor(Math.random()*ids.length)]
      }
      ch = chs.pop()
    }
  }
  this.randomDocID = function(){
    var chs = this.channelNames()
    var ch = chs[Math.floor(Math.random()*chs.length)]
    var chInfo = this.channel(ch);
    var rIds = chInfo.changes.map(function(c){return c.id})
    return rIds[Math.floor(Math.random()*rIds.length)]
  }
  this.getDoc = function(id, cb){
    client.get(["_raw", id], function(err, raw) {
      if (err) {return cb(err);}
      var deployed = raw._sync;
      delete raw._sync;
      var previewSet = {}
      var preview = runSyncFunction(previewSet, id, raw, 0)
      cb(err, raw, transformDeployed(id, deployed), transformPreview(id, preview))
    });
  }
  this.allDocs = function(cb) {
    client.get("_all_docs", function(err, data) {
      var rows = data.rows.map(function(r){
        return {id : r.id, access : previewDocs[r.id]}
      })
      cb(err, rows)
    })
  }

  // private implementation
  function transformDeployed(id, deployed){
    var access = {};
    for (var user in deployed.access) {
      var chans = Object.keys(deployed.access[user])
      for (var i = chans.length - 1; i >= 0; i--) {
        var ch = chans[i]
        access[ch] = access[ch] || []
        access[ch].push(user)
        access[ch] = access[ch].sort()
      }
    }
    return {
      access : access,
      channels : Object.keys(deployed.channels)
    }
  }

  function transformPreview(id, preview) {
    // console.log("preview", preview)
    var channelSet = {}
    preview.access.forEach(function(acc) {
      acc.channels.forEach(function(ch) {
        channelSet[ch] = channelSet[ch] || [];
        channelSet[ch] =
          mergeUsers(channelSet[ch], acc.users);
      })
    })
    console.log("preview.access", channelSet)
    return {
      access : channelSet,
      channels : preview.channels,
      reject : preview.reject
    };
  }

  function runSyncFunction(channelSet, id, doc, seq) {
    // console.log('previewFun', doc)
    doc._id = id
    var sync = previewFun(doc, false, null)
    // console.log('previewFun', doc._id, doc, sync)
    if (sync.reject) {
      console.error("update rejected by sync function", doc, sync)
      return;
    }
    var changed = {};
    previewDocs[id] = false;
    sync.channels.forEach(function(ch) {
      channelSet[ch] = channelSet[ch] || {docs : {}, access:{}};
      channelSet[ch].docs[id] = seq;
      changed[ch]=true;
    })
    sync.access.forEach(function(acc) {
      previewDocs[id] = true;
      acc.channels.forEach(function(ch){
        changed[ch]=true;
        channelSet[ch] = channelSet[ch] || {docs : {}, access:{}};
        channelSet[ch].access[id] =
          mergeUsers(channelSet[ch].access[id], acc.users);
      })
    })
    sync.changed = changed;
    return sync;
  }

  function mergeUsers(existing, more) {
    var keys = {};
    existing = existing || [];
    for (var i = existing.length - 1; i >= 0; i--) {
      keys[existing[i]] = true;
    }
    for (i = more.length - 1; i >= 0; i--) {
      keys[more[i]] = true;
    }
    return Object.keys(keys).sort()
  }

  function loadChangesHistory(){
    // get first page
    // console.log("loadChangesHistory")
    client.get(["_changes", {limit : self.pageSize, include_docs : true}], function(err, data) {
      // console.log("history", data)
      data.results.forEach(onChange)
      self.emit("batch")

      client.changes({since : data.last_seq, include_docs : true}, function(err, data){
        // console.log("change", err, data);
        if (!err)
        onChange(data)
      })
    })
  }

  self.once("batch", function() {
    self.connected = true;
    self.emit("connected")
  })
  self.on("newListener", function(name, fun){
    if (name == "connected" && self.connected) {
      fun()
    }
  })

  function onChange(ch) {
    var seq = parseInt(ch.seq.split(":")[1])
    // console.log("onChange", seq, ch)
    if (!ch.doc) {
      console.error("no doc", ch)
      return;
    }
    var sync = runSyncFunction(previewChannels, ch.id, ch.doc, seq)
    self.emit("change", ch)
    Object.keys(sync.changed).forEach(function(channel) {
      self.emit("ch:"+channel);
    })
  }

  client.get("_info", function(err, info) {
    if (err) throw(err);
    dbInfo = info;
    self.setSyncFunction(info.config.sync || "function(doc){\n  channel(doc.channels)\n}");
  })
}

util.inherits(SyncModel, events.EventEmitter);

function shuffleArray(array) {
    for (var i = array.length - 1; i > 0; i--) {
        var j = Math.floor(Math.random() * (i + 1));
        var temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
    return array;
}

var syncWrapper = function(newDoc, oldDoc, realUserCtx) {
  //syncCodeStringHere

  function makeArray(maybeArray) {
    if (Array.isArray(maybeArray)) {
      return maybeArray;
    } else {
      return [maybeArray];
    }
  }

  function inArray(string, array) {
    return array.indexOf(string) != -1;
  }

  function anyInArray(any, array) {
    for (var i = 0; i < any.length; ++i) {
      if (inArray(any[i], array))
        return true;
    }
    return false;
  }

  // Proxy userCtx that allows queries but not direct access to user/roles:
  var shouldValidate = (realUserCtx !== null && realUserCtx.name !== null);

  function requireUser(names) {
      if (!shouldValidate) return;
      names = makeArray(names);
      if (!inArray(realUserCtx.name, names))
        throw({forbidden: "wrong user"});
  }

  function requireRole(roles) {
      if (!shouldValidate) return;
      roles = makeArray(roles);
      if (!anyInArray(realUserCtx.roles, roles))
        throw({forbidden: "missing role"});
  }

  function requireAccess(channels) {
      if (!shouldValidate) return;
      channels = makeArray(channels);
      if (!anyInArray(realUserCtx.channels, channels))
        throw({forbidden: "missing channel access"});
  }
  var results = {
    channels : [],
    access : [],
    reject : false
  };
  function channel(){
    var args = Array.prototype.slice.apply(arguments);
    results.channels = Array.prototype.concat.apply(results.channels, args);
  }
  function access(users, channels){
    results.access.push({
      users : makeArray(users),
      channels : makeArray(channels)
    })
  }
  function reject(code, message) {
    results.reject = [code, message];
  }
  try {
    // console.log("syncFun", newDoc)
    syncFun(newDoc, oldDoc);
  } catch(x) {
    if (x.forbidden)
      reject(403, x.forbidden);
    else if (x.unauthorized)
      reject(401, x.unauthorized);
    else
      throw(x);
  }
  return results;
}.toString();

function compileSyncFunction(syncCode) {
  var codeString = "var syncFun = ("+ syncCode+")",
    wrappedCode = syncWrapper.replace("//syncCodeStringHere", codeString),
    evalString = "var compiledFunction = ("+ wrappedCode+")";
  eval(evalString);
  return compiledFunction;
}

},{"coax":"nf6DCT","events":10,"util":11}],"syncModel":[function(require,module,exports){
module.exports=require('f7r3xx');
},{}],"nf6DCT":[function(require,module,exports){
/*
 * coax
 * https://github.com/jchris/coax
 *
 * Copyright (c) 2013 Chris Anderson
 * Licensed under the Apache license.
 */
var pax = require("pax"),
  hoax = require("hoax");

var coaxPax = pax();

coaxPax.extend("getQuery", function(params) {
  params = JSON.parse(JSON.stringify(params));
  var key, keys = ["key", "startkey", "endkey", "start_key", "end_key"];
  for (var i = 0; i < keys.length; i++) {
    key = keys[i];
    if (params[key]) {
      params[key] = JSON.stringify(params[key]);
    }
  }
  return params;
});

var Coax = module.exports = hoax.makeHoax(coaxPax());

Coax.extend("changes", function(opts, cb) {
  if (typeof opts === "function") {
    cb = opts;
    opts = {};
  }
  var self = this;
  opts = opts || {};


  if (opts.feed == "continuous") {
    var listener = self(["_changes", opts], function(err, ok) {
      if (err && err.code == "ETIMEDOUT") {
        return self.changes(opts, cb); // TODO retry limit?
      } else if (err) {
        return cb(err);
      }
    });
    listener.on("data", function(data){
      var sep = "\n";

      // re-emit chunked json data
      eom = data.toString().indexOf(sep)
      msg = data.toString().substring(0, eom)
      remaining = data.toString().substring(eom + 1, data.length)
      if (remaining.length > 0){
        // console.log(data.toString())
        listener.emit("data", remaining)
      }

      var json = JSON.parse(msg);
      cb(false, json)
    })
    return listener;
  } else {
    opts.feed = "longpoll";
    // opts.since = opts.since || 0;
    // console.log("change opts "+JSON.stringify(opts));
    return self(["_changes", opts], function(err, ok) {
      if (err && err.code == "ETIMEDOUT") {
        return self.changes(opts, cb); // TODO retry limit?
      } else if (err) {
        return cb(err);
      }
      // console.log("changes", ok)
      ok.results.forEach(function(row){
        cb(null, row);
      });
      opts.since = ok.last_seq;
      self.changes(opts, cb);
    });
  }
});

Coax.extend("forceSave", function(doc, cb) {
  var api = this(doc._id);
  // console.log("forceSave "+api.pax);
  api.get(function(err, old) {
    if (err && err.error !== "not_found") {
      return cb(err);
    }
    if (!err) {
      doc._rev = old._rev;
    }
    // console.log("forceSave put", api.pax, doc._rev)
    api.put(doc, cb);
  });
});


Coax.extend("channels", function(channels, opts) {
  var self = this;
  var opts = opts || {};

  opts.filter = "sync_gateway/bychannel";
  opts.feed = "continuous"
  opts.channels = channels.join(',')

  // console.log(self.pax.toString())
  var x = function(){};
  x.request = true;
  var changes = self(['_changes', opts], x);
  changes.on("data", function(data) {
    var json;
    try{
      var json = JSON.parse(data.toString())
    }catch(e){
      console.log("not json", data.toString())
    }
    if (json) {
      changes.emit("json", json)
    }
  })
  return changes;
});

},{"hoax":5,"pax":8}],"coax":[function(require,module,exports){
module.exports=require('nf6DCT');
},{}],5:[function(require,module,exports){
var core = require("./hoax-core"),
  request = require("browser-request");

request.log.debug = function() {};

module.exports = core(request);

},{"./hoax-core":6,"browser-request":7}],6:[function(require,module,exports){
/*
 * hoax
 * https://github.com/jchris/hoax
 *
 * Copyright (c) 2013 Chris Anderson
 * Licensed under the Apache license.
 */

module.exports = function(request) {
  var pax = require("pax");

  function makeHoaxCallback(cb, verb) {
    return function(err, res, body){
      // console.log("hoax cb", verb||"get", err, res.statusCode, body);
      if (err && err !== "error") {
        cb(err, res, body);
      } else {
        if (res.statusCode >= 400 || err === "error") {
          cb(body || res.statusCode, res);
        } else {
          cb(null, body);
        }
      }
    };
  }

  function processArguments(myPax, urlOrOpts, data, cb, verb) {
    var opts = {}, newPax = myPax;
    if (typeof urlOrOpts === 'function') {
      cb = urlOrOpts;
      data = null;
      urlOrOpts = null;
    } else {
      if (urlOrOpts.uri || urlOrOpts.url) {
        newPax = myPax(urlOrOpts.uri || urlOrOpts.url);
      } else {
        if (typeof data === 'function') {
          // we have only 2 args
          // the first is data if it is not an array
          // and the verb is put or post
          cb = data;
          data = null;
          if ((verb === "put" || verb === "post") &&
            (typeof urlOrOpts !== "string" &&
              Object.prototype.toString.call(urlOrOpts) !== '[object Array]')) {
              data = urlOrOpts;
          } else {
            newPax = myPax(urlOrOpts);
          }
        } else {
          newPax = myPax(urlOrOpts);
        }
      }
    }
    opts.headers = {'content-type': 'application/json'};
    opts.json = true;
    opts.uri = newPax.toString();
    if (data) {
      opts.body = JSON.stringify(data);
    }
    return [opts, cb, newPax];
  }

  function extenderizer(oldHoax) {
    return function(name, fun) {
      this.methods = this.methods || {};
      this.methods[name] = fun;
      this[name] = fun;
    };
  }

  function addExtensions(newHoax, oldHoax) {
    if (oldHoax && oldHoax.methods) {
      var k;
      for (k in oldHoax.methods) {
        newHoax[k] = oldHoax.methods[k];
      }
    }
  }

  function makeHoax(myPax, verb, oldHoax) {
    var newHoax = function(opts, data, xcb) {
      var args = processArguments(myPax, opts, data, xcb, verb),
        reqOpts = args[0], // includes uri, body
        cb = args[1],
        newPax = args[2];
      if (cb) {
        // console.log(["hoax", verb||"get", reqOpts]);
        if (verb) {
          if (verb == "del") {
            reqOpts.method = "DELETE";
          } else {
            reqOpts.method = verb.toUpperCase();
          }
          return request(reqOpts, makeHoaxCallback(cb, verb));
        } else {
          return request(reqOpts, makeHoaxCallback(cb));
        }
      } else {
        // console.log("new hoax", newPax);
        return makeHoax(newPax, verb, newHoax);
      }
    };
    if (!verb) {
      "get put post head del".split(" ").forEach(function(v){
        newHoax[v] = makeHoax(myPax, v, newHoax);
      });
    }
    addExtensions(newHoax, oldHoax);
    // should this be extenderizer(newHoax) ?
    newHoax.extend = extenderizer(oldHoax);
    newHoax.pax = myPax; // deprecated
    newHoax.url = myPax;
    return newHoax;
  }

  var Hoax = makeHoax(pax());
  Hoax.makeHoax = makeHoax;

  return Hoax;
};

},{"pax":8}],7:[function(require,module,exports){
// Browser Request
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var XHR = XMLHttpRequest
if (!XHR) throw new Error('missing XMLHttpRequest')

module.exports = request
request.log = {
  'trace': noop, 'debug': noop, 'info': noop, 'warn': noop, 'error': noop
}

var DEFAULT_TIMEOUT = 3 * 60 * 1000 // 3 minutes

//
// request
//

function request(options, callback) {
  // The entry-point to the API: prep the options object and pass the real work to run_xhr.
  if(typeof callback !== 'function')
    throw new Error('Bad callback given: ' + callback)

  if(!options)
    throw new Error('No options given')

  var options_onResponse = options.onResponse; // Save this for later.

  if(typeof options === 'string')
    options = {'uri':options};
  else
    options = JSON.parse(JSON.stringify(options)); // Use a duplicate for mutating.

  options.onResponse = options_onResponse // And put it back.

  if (options.verbose) request.log = getLogger();

  if(options.url) {
    options.uri = options.url;
    delete options.url;
  }

  if(!options.uri && options.uri !== "")
    throw new Error("options.uri is a required argument");

  if(typeof options.uri != "string")
    throw new Error("options.uri must be a string");

  var unsupported_options = ['proxy', '_redirectsFollowed', 'maxRedirects', 'followRedirect']
  for (var i = 0; i < unsupported_options.length; i++)
    if(options[ unsupported_options[i] ])
      throw new Error("options." + unsupported_options[i] + " is not supported")

  options.callback = callback
  options.method = options.method || 'GET';
  options.headers = options.headers || {};
  options.body    = options.body || null
  options.timeout = options.timeout || request.DEFAULT_TIMEOUT

  if(options.headers.host)
    throw new Error("Options.headers.host is not supported");

  if(options.json) {
    options.headers.accept = options.headers.accept || 'application/json'
    if(options.method !== 'GET')
      options.headers['content-type'] = 'application/json'

    if(typeof options.json !== 'boolean')
      options.body = JSON.stringify(options.json)
    else if(typeof options.body !== 'string')
      options.body = JSON.stringify(options.body)
  }

  // If onResponse is boolean true, call back immediately when the response is known,
  // not when the full request is complete.
  options.onResponse = options.onResponse || noop
  if(options.onResponse === true) {
    options.onResponse = callback
    options.callback = noop
  }

  // XXX Browsers do not like this.
  //if(options.body)
  //  options.headers['content-length'] = options.body.length;

  // HTTP basic authentication
  if(!options.headers.authorization && options.auth)
    options.headers.authorization = 'Basic ' + b64_enc(options.auth.username + ':' + options.auth.password);

  return run_xhr(options)
}

var req_seq = 0
function run_xhr(options) {
  var xhr = new XHR
    , timed_out = false
    , is_cors = is_crossDomain(options.uri)
    , supports_cors = ('withCredentials' in xhr)

  req_seq += 1
  xhr.seq_id = req_seq
  xhr.id = req_seq + ': ' + options.method + ' ' + options.uri
  xhr._id = xhr.id // I know I will type "_id" from habit all the time.

  if(is_cors && !supports_cors) {
    var cors_err = new Error('Browser does not support cross-origin request: ' + options.uri)
    cors_err.cors = 'unsupported'
    return options.callback(cors_err, xhr)
  }

  xhr.timeoutTimer = setTimeout(too_late, options.timeout)
  function too_late() {
    timed_out = true
    var er = new Error('ETIMEDOUT')
    er.code = 'ETIMEDOUT'
    er.duration = options.timeout

    request.log.error('Timeout', { 'id':xhr._id, 'milliseconds':options.timeout })
    return options.callback(er, xhr)
  }

  // Some states can be skipped over, so remember what is still incomplete.
  var did = {'response':false, 'loading':false, 'end':false}

  xhr.onreadystatechange = on_state_change
  xhr.open(options.method, options.uri, true) // asynchronous
  if(is_cors)
    xhr.withCredentials = !! options.withCredentials
  xhr.send(options.body)
  return xhr

  function on_state_change(event) {
    if(timed_out)
      return request.log.debug('Ignoring timed out state change', {'state':xhr.readyState, 'id':xhr.id})

    request.log.debug('State change', {'state':xhr.readyState, 'id':xhr.id, 'timed_out':timed_out})

    if(xhr.readyState === XHR.OPENED) {
      request.log.debug('Request started', {'id':xhr.id})
      for (var key in options.headers)
        xhr.setRequestHeader(key, options.headers[key])
    }

    else if(xhr.readyState === XHR.HEADERS_RECEIVED)
      on_response()

    else if(xhr.readyState === XHR.LOADING) {
      on_response()
      on_loading()
    }

    else if(xhr.readyState === XHR.DONE) {
      on_response()
      on_loading()
      on_end()
    }
  }

  function on_response() {
    if(did.response)
      return

    did.response = true
    request.log.debug('Got response', {'id':xhr.id, 'status':xhr.status})
    clearTimeout(xhr.timeoutTimer)
    xhr.statusCode = xhr.status // Node request compatibility

    // Detect failed CORS requests.
    if(is_cors && xhr.statusCode == 0) {
      var cors_err = new Error('CORS request rejected: ' + options.uri)
      cors_err.cors = 'rejected'

      // Do not process this request further.
      did.loading = true
      did.end = true

      return options.callback(cors_err, xhr)
    }

    options.onResponse(null, xhr)
  }

  function on_loading() {
    if(did.loading)
      return

    did.loading = true
    request.log.debug('Response body loading', {'id':xhr.id})
    // TODO: Maybe simulate "data" events by watching xhr.responseText
  }

  function on_end() {
    if(did.end)
      return

    did.end = true
    request.log.debug('Request done', {'id':xhr.id})

    xhr.body = xhr.responseText
    if(options.json) {
      try        { xhr.body = JSON.parse(xhr.responseText) }
      catch (er) { return options.callback(er, xhr)        }
    }

    options.callback(null, xhr, xhr.body)
  }

} // request

request.withCredentials = false;
request.DEFAULT_TIMEOUT = DEFAULT_TIMEOUT;

//
// defaults
//

request.defaults = function(options, requester) {
  var def = function (method) {
    var d = function (params, callback) {
      if(typeof params === 'string')
        params = {'uri': params};
      else {
        params = JSON.parse(JSON.stringify(params));
      }
      for (var i in options) {
        if (params[i] === undefined) params[i] = options[i]
      }
      return method(params, callback)
    }
    return d
  }
  var de = def(request)
  de.get = def(request.get)
  de.post = def(request.post)
  de.put = def(request.put)
  de.head = def(request.head)
  return de
}

//
// HTTP method shortcuts
//

var shortcuts = [ 'get', 'put', 'post', 'head' ];
shortcuts.forEach(function(shortcut) {
  var method = shortcut.toUpperCase();
  var func   = shortcut.toLowerCase();

  request[func] = function(opts) {
    if(typeof opts === 'string')
      opts = {'method':method, 'uri':opts};
    else {
      opts = JSON.parse(JSON.stringify(opts));
      opts.method = method;
    }

    var args = [opts].concat(Array.prototype.slice.apply(arguments, [1]));
    return request.apply(this, args);
  }
})

//
// CouchDB shortcut
//

request.couch = function(options, callback) {
  if(typeof options === 'string')
    options = {'uri':options}

  // Just use the request API to do JSON.
  options.json = true
  if(options.body)
    options.json = options.body
  delete options.body

  callback = callback || noop

  var xhr = request(options, couch_handler)
  return xhr

  function couch_handler(er, resp, body) {
    if(er)
      return callback(er, resp, body)

    if((resp.statusCode < 200 || resp.statusCode > 299) && body.error) {
      // The body is a Couch JSON object indicating the error.
      er = new Error('CouchDB error: ' + (body.error.reason || body.error.error))
      for (var key in body)
        er[key] = body[key]
      return callback(er, resp, body);
    }

    return callback(er, resp, body);
  }
}

//
// Utility
//

function noop() {}

function getLogger() {
  var logger = {}
    , levels = ['trace', 'debug', 'info', 'warn', 'error']
    , level, i

  for(i = 0; i < levels.length; i++) {
    level = levels[i]

    logger[level] = noop
    if(typeof console !== 'undefined' && console && console[level])
      logger[level] = formatted(console, level)
  }

  return logger
}

function formatted(obj, method) {
  return formatted_logger

  function formatted_logger(str, context) {
    if(typeof context === 'object')
      str += ' ' + JSON.stringify(context)

    return obj[method].call(obj, str)
  }
}

// Return whether a URL is a cross-domain request.
function is_crossDomain(url) {
  var rurl = /^([\w\+\.\-]+:)(?:\/\/([^\/?#:]*)(?::(\d+))?)?/

  // jQuery #8138, IE may throw an exception when accessing
  // a field from window.location if document.domain has been set
  var ajaxLocation
  try { ajaxLocation = location.href }
  catch (e) {
    // Use the href attribute of an A element since IE will modify it given document.location
    ajaxLocation = document.createElement( "a" );
    ajaxLocation.href = "";
    ajaxLocation = ajaxLocation.href;
  }

  var ajaxLocParts = rurl.exec(ajaxLocation.toLowerCase()) || []
    , parts = rurl.exec(url.toLowerCase() )

  var result = !!(
    parts &&
    (  parts[1] != ajaxLocParts[1]
    || parts[2] != ajaxLocParts[2]
    || (parts[3] || (parts[1] === "http:" ? 80 : 443)) != (ajaxLocParts[3] || (ajaxLocParts[1] === "http:" ? 80 : 443))
    )
  )

  //console.debug('is_crossDomain('+url+') -> ' + result)
  return result
}

// MIT License from http://phpjs.org/functions/base64_encode:358
function b64_enc (data) {
    // Encodes string using MIME base64 algorithm
    var b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    var o1, o2, o3, h1, h2, h3, h4, bits, i = 0, ac = 0, enc="", tmp_arr = [];

    if (!data) {
        return data;
    }

    // assume utf8 data
    // data = this.utf8_encode(data+'');

    do { // pack three octets into four hexets
        o1 = data.charCodeAt(i++);
        o2 = data.charCodeAt(i++);
        o3 = data.charCodeAt(i++);

        bits = o1<<16 | o2<<8 | o3;

        h1 = bits>>18 & 0x3f;
        h2 = bits>>12 & 0x3f;
        h3 = bits>>6 & 0x3f;
        h4 = bits & 0x3f;

        // use hexets to index into b64, and append result to encoded string
        tmp_arr[ac++] = b64.charAt(h1) + b64.charAt(h2) + b64.charAt(h3) + b64.charAt(h4);
    } while (i < data.length);

    enc = tmp_arr.join('');

    switch (data.length % 3) {
        case 1:
            enc = enc.slice(0, -2) + '==';
        break;
        case 2:
            enc = enc.slice(0, -1) + '=';
        break;
    }

    return enc;
}

},{}],8:[function(require,module,exports){
/*
 * pax
 * https://github.com/jchris/pax
 *
 * Copyright (c) 2013 Chris Anderson
 * Licensed under the APL license.
 */

function objToQuery(q) {
  var k, ks = Object.keys(q), v, query = [];
  for (k = 0; k < ks.length; k++) {
    v = q[ks[k]];
    query.push(encodeURIComponent(ks[k])+'='+encodeURIComponent(v.toString()));
  }
  return query.join('&');
}

// if there is an object in the new path,
// pluck it out and put it on the pax instance;

function processPath(path) {
  var query;
  if (path && path.pop && path.length) {
    if (typeof path[path.length-1] === 'object') {
      path.query = path.pop();
    }
    return path;
  } else if (typeof path === "object") { // options
    var empty = [];
    empty.query = path;
    return empty;
  } else if (path) { // string
    return [path];
  } else {
    return [];
  }
}

function merge(target, source) {
  for (var key in source) {
    if (source.hasOwnProperty(key)) {
      target[key] = source[key];
    }
  }
  return target;
}

function mergePaths(path, newPath) {
  var k, merged = path.concat(newPath);
  merged.methods = {};
  if (path.query)  {
    merged.query = merge({}, path.query);
  }
  if (newPath.query) {
    merged.query = merge(merged.query || {}, newPath.query);
  }
  if (typeof path.getQuery !== 'undefined') {
    merged.getQuery = path.getQuery;
  }
  for (k in path.methods) {
    merged.methods[k] = path.methods[k];
  }

  // if (typeof newPath.getQuery !== 'undefined') {
  //   merged.getQuery = newPath.getQuery;
  // }
  return merged;
}

function makeToString(path) {
  var first = true,
  encoded = path.map(function(p) {
    if (first) {
      first = false;
      if (/^http/.test(p)) {
        if (/\/$/.test(p)) {
          return p.substring(0,p.length-1);
        } else {
          return p;
        }
      }
    }
    return encodeURIComponent(p);
  });

  return function() {
    if (path.query) {
      var qobj;
      if (path.getQuery || this.getQuery) {
        qobj = (path.getQuery || this.getQuery)(path.query);
      } else {
        qobj = path.query;
      }
      return encoded.join('/') + '?' + objToQuery(qobj);
    } else {
      return encoded.join('/');
    }
  };
}

function extenderizer(path) {
  path.methods = path.methods || {};
  return function(name, fun) {
    path.methods[name] = fun;
    this[name] = fun;
  };
}

function addExtensions(pax, path) {
  var k;
  for (k in path.methods) {
    pax[k] = path.methods[k];
  }
}

var growPax;

function makeNextPathFun(path) {
  var nextPax = function(nextPath) {
    // console.log("nextPax",nextPax);
    if (typeof nextPax.getQuery !== 'undefined') {path.getQuery = nextPax.getQuery;}
    if (arguments.length > 1) {
      return growPax(path, [].map.call(arguments,function(arg){return arg;}));
    } else {
      return growPax(path, nextPath);
    }
  };
  addExtensions(nextPax, path);
  nextPax.extend = extenderizer(path);
  // console.log(["pax", path, path.query]);
  nextPax.toString = makeToString(path);
  // console.log(["paxs", nextPax.toString()]);
  return nextPax;
}

function growPax(path, newPath) {
  newPath = processPath(newPath);
  path = mergePaths(path, newPath);
  return makeNextPathFun(path);
}

module.exports = makeNextPathFun([]);


},{}],9:[function(require,module,exports){


//
// The shims in this file are not fully implemented shims for the ES5
// features, but do work for the particular usecases there is in
// the other modules.
//

var toString = Object.prototype.toString;
var hasOwnProperty = Object.prototype.hasOwnProperty;

// Array.isArray is supported in IE9
function isArray(xs) {
  return toString.call(xs) === '[object Array]';
}
exports.isArray = typeof Array.isArray === 'function' ? Array.isArray : isArray;

// Array.prototype.indexOf is supported in IE9
exports.indexOf = function indexOf(xs, x) {
  if (xs.indexOf) return xs.indexOf(x);
  for (var i = 0; i < xs.length; i++) {
    if (x === xs[i]) return i;
  }
  return -1;
};

// Array.prototype.filter is supported in IE9
exports.filter = function filter(xs, fn) {
  if (xs.filter) return xs.filter(fn);
  var res = [];
  for (var i = 0; i < xs.length; i++) {
    if (fn(xs[i], i, xs)) res.push(xs[i]);
  }
  return res;
};

// Array.prototype.forEach is supported in IE9
exports.forEach = function forEach(xs, fn, self) {
  if (xs.forEach) return xs.forEach(fn, self);
  for (var i = 0; i < xs.length; i++) {
    fn.call(self, xs[i], i, xs);
  }
};

// Array.prototype.map is supported in IE9
exports.map = function map(xs, fn) {
  if (xs.map) return xs.map(fn);
  var out = new Array(xs.length);
  for (var i = 0; i < xs.length; i++) {
    out[i] = fn(xs[i], i, xs);
  }
  return out;
};

// Array.prototype.reduce is supported in IE9
exports.reduce = function reduce(array, callback, opt_initialValue) {
  if (array.reduce) return array.reduce(callback, opt_initialValue);
  var value, isValueSet = false;

  if (2 < arguments.length) {
    value = opt_initialValue;
    isValueSet = true;
  }
  for (var i = 0, l = array.length; l > i; ++i) {
    if (array.hasOwnProperty(i)) {
      if (isValueSet) {
        value = callback(value, array[i], i, array);
      }
      else {
        value = array[i];
        isValueSet = true;
      }
    }
  }

  return value;
};

// String.prototype.substr - negative index don't work in IE8
if ('ab'.substr(-1) !== 'b') {
  exports.substr = function (str, start, length) {
    // did we get a negative start, calculate how much it is from the beginning of the string
    if (start < 0) start = str.length + start;

    // call the original function
    return str.substr(start, length);
  };
} else {
  exports.substr = function (str, start, length) {
    return str.substr(start, length);
  };
}

// String.prototype.trim is supported in IE9
exports.trim = function (str) {
  if (str.trim) return str.trim();
  return str.replace(/^\s+|\s+$/g, '');
};

// Function.prototype.bind is supported in IE9
exports.bind = function () {
  var args = Array.prototype.slice.call(arguments);
  var fn = args.shift();
  if (fn.bind) return fn.bind.apply(fn, args);
  var self = args.shift();
  return function () {
    fn.apply(self, args.concat([Array.prototype.slice.call(arguments)]));
  };
};

// Object.create is supported in IE9
function create(prototype, properties) {
  var object;
  if (prototype === null) {
    object = { '__proto__' : null };
  }
  else {
    if (typeof prototype !== 'object') {
      throw new TypeError(
        'typeof prototype[' + (typeof prototype) + '] != \'object\''
      );
    }
    var Type = function () {};
    Type.prototype = prototype;
    object = new Type();
    object.__proto__ = prototype;
  }
  if (typeof properties !== 'undefined' && Object.defineProperties) {
    Object.defineProperties(object, properties);
  }
  return object;
}
exports.create = typeof Object.create === 'function' ? Object.create : create;

// Object.keys and Object.getOwnPropertyNames is supported in IE9 however
// they do show a description and number property on Error objects
function notObject(object) {
  return ((typeof object != "object" && typeof object != "function") || object === null);
}

function keysShim(object) {
  if (notObject(object)) {
    throw new TypeError("Object.keys called on a non-object");
  }

  var result = [];
  for (var name in object) {
    if (hasOwnProperty.call(object, name)) {
      result.push(name);
    }
  }
  return result;
}

// getOwnPropertyNames is almost the same as Object.keys one key feature
//  is that it returns hidden properties, since that can't be implemented,
//  this feature gets reduced so it just shows the length property on arrays
function propertyShim(object) {
  if (notObject(object)) {
    throw new TypeError("Object.getOwnPropertyNames called on a non-object");
  }

  var result = keysShim(object);
  if (exports.isArray(object) && exports.indexOf(object, 'length') === -1) {
    result.push('length');
  }
  return result;
}

var keys = typeof Object.keys === 'function' ? Object.keys : keysShim;
var getOwnPropertyNames = typeof Object.getOwnPropertyNames === 'function' ?
  Object.getOwnPropertyNames : propertyShim;

if (new Error().hasOwnProperty('description')) {
  var ERROR_PROPERTY_FILTER = function (obj, array) {
    if (toString.call(obj) === '[object Error]') {
      array = exports.filter(array, function (name) {
        return name !== 'description' && name !== 'number' && name !== 'message';
      });
    }
    return array;
  };

  exports.keys = function (object) {
    return ERROR_PROPERTY_FILTER(object, keys(object));
  };
  exports.getOwnPropertyNames = function (object) {
    return ERROR_PROPERTY_FILTER(object, getOwnPropertyNames(object));
  };
} else {
  exports.keys = keys;
  exports.getOwnPropertyNames = getOwnPropertyNames;
}

// Object.getOwnPropertyDescriptor - supported in IE8 but only on dom elements
function valueObject(value, key) {
  return { value: value[key] };
}

if (typeof Object.getOwnPropertyDescriptor === 'function') {
  try {
    Object.getOwnPropertyDescriptor({'a': 1}, 'a');
    exports.getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
  } catch (e) {
    // IE8 dom element issue - use a try catch and default to valueObject
    exports.getOwnPropertyDescriptor = function (value, key) {
      try {
        return Object.getOwnPropertyDescriptor(value, key);
      } catch (e) {
        return valueObject(value, key);
      }
    };
  }
} else {
  exports.getOwnPropertyDescriptor = valueObject;
}

},{}],10:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var util = require('util');

function EventEmitter() {
  this._events = this._events || {};
  this._maxListeners = this._maxListeners || undefined;
}
module.exports = EventEmitter;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
EventEmitter.defaultMaxListeners = 10;

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function(n) {
  if (!util.isNumber(n) || n < 0)
    throw TypeError('n must be a positive number');
  this._maxListeners = n;
  return this;
};

EventEmitter.prototype.emit = function(type) {
  var er, handler, len, args, i, listeners;

  if (!this._events)
    this._events = {};

  // If there is no 'error' event listener then throw.
  if (type === 'error') {
    if (!this._events.error ||
        (util.isObject(this._events.error) && !this._events.error.length)) {
      er = arguments[1];
      if (er instanceof Error) {
        throw er; // Unhandled 'error' event
      } else {
        throw TypeError('Uncaught, unspecified "error" event.');
      }
      return false;
    }
  }

  handler = this._events[type];

  if (util.isUndefined(handler))
    return false;

  if (util.isFunction(handler)) {
    switch (arguments.length) {
      // fast cases
      case 1:
        handler.call(this);
        break;
      case 2:
        handler.call(this, arguments[1]);
        break;
      case 3:
        handler.call(this, arguments[1], arguments[2]);
        break;
      // slower
      default:
        len = arguments.length;
        args = new Array(len - 1);
        for (i = 1; i < len; i++)
          args[i - 1] = arguments[i];
        handler.apply(this, args);
    }
  } else if (util.isObject(handler)) {
    len = arguments.length;
    args = new Array(len - 1);
    for (i = 1; i < len; i++)
      args[i - 1] = arguments[i];

    listeners = handler.slice();
    len = listeners.length;
    for (i = 0; i < len; i++)
      listeners[i].apply(this, args);
  }

  return true;
};

EventEmitter.prototype.addListener = function(type, listener) {
  var m;

  if (!util.isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events)
    this._events = {};

  // To avoid recursion in the case that type === "newListener"! Before
  // adding it to the listeners, first emit "newListener".
  if (this._events.newListener)
    this.emit('newListener', type,
              util.isFunction(listener.listener) ?
              listener.listener : listener);

  if (!this._events[type])
    // Optimize the case of one listener. Don't need the extra array object.
    this._events[type] = listener;
  else if (util.isObject(this._events[type]))
    // If we've already got an array, just append.
    this._events[type].push(listener);
  else
    // Adding the second element, need to change to array.
    this._events[type] = [this._events[type], listener];

  // Check for listener leak
  if (util.isObject(this._events[type]) && !this._events[type].warned) {
    var m;
    if (!util.isUndefined(this._maxListeners)) {
      m = this._maxListeners;
    } else {
      m = EventEmitter.defaultMaxListeners;
    }

    if (m && m > 0 && this._events[type].length > m) {
      this._events[type].warned = true;
      console.error('(node) warning: possible EventEmitter memory ' +
                    'leak detected. %d listeners added. ' +
                    'Use emitter.setMaxListeners() to increase limit.',
                    this._events[type].length);
      console.trace();
    }
  }

  return this;
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.once = function(type, listener) {
  if (!util.isFunction(listener))
    throw TypeError('listener must be a function');

  function g() {
    this.removeListener(type, g);
    listener.apply(this, arguments);
  }

  g.listener = listener;
  this.on(type, g);

  return this;
};

// emits a 'removeListener' event iff the listener was removed
EventEmitter.prototype.removeListener = function(type, listener) {
  var list, position, length, i;

  if (!util.isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events || !this._events[type])
    return this;

  list = this._events[type];
  length = list.length;
  position = -1;

  if (list === listener ||
      (util.isFunction(list.listener) && list.listener === listener)) {
    delete this._events[type];
    if (this._events.removeListener)
      this.emit('removeListener', type, listener);

  } else if (util.isObject(list)) {
    for (i = length; i-- > 0;) {
      if (list[i] === listener ||
          (list[i].listener && list[i].listener === listener)) {
        position = i;
        break;
      }
    }

    if (position < 0)
      return this;

    if (list.length === 1) {
      list.length = 0;
      delete this._events[type];
    } else {
      list.splice(position, 1);
    }

    if (this._events.removeListener)
      this.emit('removeListener', type, listener);
  }

  return this;
};

EventEmitter.prototype.removeAllListeners = function(type) {
  var key, listeners;

  if (!this._events)
    return this;

  // not listening for removeListener, no need to emit
  if (!this._events.removeListener) {
    if (arguments.length === 0)
      this._events = {};
    else if (this._events[type])
      delete this._events[type];
    return this;
  }

  // emit removeListener for all listeners on all events
  if (arguments.length === 0) {
    for (key in this._events) {
      if (key === 'removeListener') continue;
      this.removeAllListeners(key);
    }
    this.removeAllListeners('removeListener');
    this._events = {};
    return this;
  }

  listeners = this._events[type];

  if (util.isFunction(listeners)) {
    this.removeListener(type, listeners);
  } else {
    // LIFO order
    while (listeners.length)
      this.removeListener(type, listeners[listeners.length - 1]);
  }
  delete this._events[type];

  return this;
};

EventEmitter.prototype.listeners = function(type) {
  var ret;
  if (!this._events || !this._events[type])
    ret = [];
  else if (util.isFunction(this._events[type]))
    ret = [this._events[type]];
  else
    ret = this._events[type].slice();
  return ret;
};

EventEmitter.listenerCount = function(emitter, type) {
  var ret;
  if (!emitter._events || !emitter._events[type])
    ret = 0;
  else if (util.isFunction(emitter._events[type]))
    ret = 1;
  else
    ret = emitter._events[type].length;
  return ret;
};
},{"util":11}],11:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var shims = require('_shims');

var formatRegExp = /%[sdj%]/g;
exports.format = function(f) {
  if (!isString(f)) {
    var objects = [];
    for (var i = 0; i < arguments.length; i++) {
      objects.push(inspect(arguments[i]));
    }
    return objects.join(' ');
  }

  var i = 1;
  var args = arguments;
  var len = args.length;
  var str = String(f).replace(formatRegExp, function(x) {
    if (x === '%%') return '%';
    if (i >= len) return x;
    switch (x) {
      case '%s': return String(args[i++]);
      case '%d': return Number(args[i++]);
      case '%j':
        try {
          return JSON.stringify(args[i++]);
        } catch (_) {
          return '[Circular]';
        }
      default:
        return x;
    }
  });
  for (var x = args[i]; i < len; x = args[++i]) {
    if (isNull(x) || !isObject(x)) {
      str += ' ' + x;
    } else {
      str += ' ' + inspect(x);
    }
  }
  return str;
};

/**
 * Echos the value of a value. Trys to print the value out
 * in the best way possible given the different types.
 *
 * @param {Object} obj The object to print out.
 * @param {Object} opts Optional options object that alters the output.
 */
/* legacy: obj, showHidden, depth, colors*/
function inspect(obj, opts) {
  // default options
  var ctx = {
    seen: [],
    stylize: stylizeNoColor
  };
  // legacy...
  if (arguments.length >= 3) ctx.depth = arguments[2];
  if (arguments.length >= 4) ctx.colors = arguments[3];
  if (isBoolean(opts)) {
    // legacy...
    ctx.showHidden = opts;
  } else if (opts) {
    // got an "options" object
    exports._extend(ctx, opts);
  }
  // set default options
  if (isUndefined(ctx.showHidden)) ctx.showHidden = false;
  if (isUndefined(ctx.depth)) ctx.depth = 2;
  if (isUndefined(ctx.colors)) ctx.colors = false;
  if (isUndefined(ctx.customInspect)) ctx.customInspect = true;
  if (ctx.colors) ctx.stylize = stylizeWithColor;
  return formatValue(ctx, obj, ctx.depth);
}
exports.inspect = inspect;


// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
inspect.colors = {
  'bold' : [1, 22],
  'italic' : [3, 23],
  'underline' : [4, 24],
  'inverse' : [7, 27],
  'white' : [37, 39],
  'grey' : [90, 39],
  'black' : [30, 39],
  'blue' : [34, 39],
  'cyan' : [36, 39],
  'green' : [32, 39],
  'magenta' : [35, 39],
  'red' : [31, 39],
  'yellow' : [33, 39]
};

// Don't use 'blue' not visible on cmd.exe
inspect.styles = {
  'special': 'cyan',
  'number': 'yellow',
  'boolean': 'yellow',
  'undefined': 'grey',
  'null': 'bold',
  'string': 'green',
  'date': 'magenta',
  // "name": intentionally not styling
  'regexp': 'red'
};


function stylizeWithColor(str, styleType) {
  var style = inspect.styles[styleType];

  if (style) {
    return '\u001b[' + inspect.colors[style][0] + 'm' + str +
           '\u001b[' + inspect.colors[style][1] + 'm';
  } else {
    return str;
  }
}


function stylizeNoColor(str, styleType) {
  return str;
}


function arrayToHash(array) {
  var hash = {};

  shims.forEach(array, function(val, idx) {
    hash[val] = true;
  });

  return hash;
}


function formatValue(ctx, value, recurseTimes) {
  // Provide a hook for user-specified inspect functions.
  // Check that value is an object with an inspect function on it
  if (ctx.customInspect &&
      value &&
      isFunction(value.inspect) &&
      // Filter out the util module, it's inspect function is special
      value.inspect !== exports.inspect &&
      // Also filter out any prototype objects using the circular check.
      !(value.constructor && value.constructor.prototype === value)) {
    var ret = value.inspect(recurseTimes);
    if (!isString(ret)) {
      ret = formatValue(ctx, ret, recurseTimes);
    }
    return ret;
  }

  // Primitive types cannot have properties
  var primitive = formatPrimitive(ctx, value);
  if (primitive) {
    return primitive;
  }

  // Look up the keys of the object.
  var keys = shims.keys(value);
  var visibleKeys = arrayToHash(keys);

  if (ctx.showHidden) {
    keys = shims.getOwnPropertyNames(value);
  }

  // Some type of object without properties can be shortcutted.
  if (keys.length === 0) {
    if (isFunction(value)) {
      var name = value.name ? ': ' + value.name : '';
      return ctx.stylize('[Function' + name + ']', 'special');
    }
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    }
    if (isDate(value)) {
      return ctx.stylize(Date.prototype.toString.call(value), 'date');
    }
    if (isError(value)) {
      return formatError(value);
    }
  }

  var base = '', array = false, braces = ['{', '}'];

  // Make Array say that they are Array
  if (isArray(value)) {
    array = true;
    braces = ['[', ']'];
  }

  // Make functions say that they are functions
  if (isFunction(value)) {
    var n = value.name ? ': ' + value.name : '';
    base = ' [Function' + n + ']';
  }

  // Make RegExps say that they are RegExps
  if (isRegExp(value)) {
    base = ' ' + RegExp.prototype.toString.call(value);
  }

  // Make dates with properties first say the date
  if (isDate(value)) {
    base = ' ' + Date.prototype.toUTCString.call(value);
  }

  // Make error with message first say the error
  if (isError(value)) {
    base = ' ' + formatError(value);
  }

  if (keys.length === 0 && (!array || value.length == 0)) {
    return braces[0] + base + braces[1];
  }

  if (recurseTimes < 0) {
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    } else {
      return ctx.stylize('[Object]', 'special');
    }
  }

  ctx.seen.push(value);

  var output;
  if (array) {
    output = formatArray(ctx, value, recurseTimes, visibleKeys, keys);
  } else {
    output = keys.map(function(key) {
      return formatProperty(ctx, value, recurseTimes, visibleKeys, key, array);
    });
  }

  ctx.seen.pop();

  return reduceToSingleString(output, base, braces);
}


function formatPrimitive(ctx, value) {
  if (isUndefined(value))
    return ctx.stylize('undefined', 'undefined');
  if (isString(value)) {
    var simple = '\'' + JSON.stringify(value).replace(/^"|"$/g, '')
                                             .replace(/'/g, "\\'")
                                             .replace(/\\"/g, '"') + '\'';
    return ctx.stylize(simple, 'string');
  }
  if (isNumber(value))
    return ctx.stylize('' + value, 'number');
  if (isBoolean(value))
    return ctx.stylize('' + value, 'boolean');
  // For some reason typeof null is "object", so special case here.
  if (isNull(value))
    return ctx.stylize('null', 'null');
}


function formatError(value) {
  return '[' + Error.prototype.toString.call(value) + ']';
}


function formatArray(ctx, value, recurseTimes, visibleKeys, keys) {
  var output = [];
  for (var i = 0, l = value.length; i < l; ++i) {
    if (hasOwnProperty(value, String(i))) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          String(i), true));
    } else {
      output.push('');
    }
  }

  shims.forEach(keys, function(key) {
    if (!key.match(/^\d+$/)) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          key, true));
    }
  });
  return output;
}


function formatProperty(ctx, value, recurseTimes, visibleKeys, key, array) {
  var name, str, desc;
  desc = shims.getOwnPropertyDescriptor(value, key) || { value: value[key] };
  if (desc.get) {
    if (desc.set) {
      str = ctx.stylize('[Getter/Setter]', 'special');
    } else {
      str = ctx.stylize('[Getter]', 'special');
    }
  } else {
    if (desc.set) {
      str = ctx.stylize('[Setter]', 'special');
    }
  }

  if (!hasOwnProperty(visibleKeys, key)) {
    name = '[' + key + ']';
  }
  if (!str) {
    if (shims.indexOf(ctx.seen, desc.value) < 0) {
      if (isNull(recurseTimes)) {
        str = formatValue(ctx, desc.value, null);
      } else {
        str = formatValue(ctx, desc.value, recurseTimes - 1);
      }
      if (str.indexOf('\n') > -1) {
        if (array) {
          str = str.split('\n').map(function(line) {
            return '  ' + line;
          }).join('\n').substr(2);
        } else {
          str = '\n' + str.split('\n').map(function(line) {
            return '   ' + line;
          }).join('\n');
        }
      }
    } else {
      str = ctx.stylize('[Circular]', 'special');
    }
  }
  if (isUndefined(name)) {
    if (array && key.match(/^\d+$/)) {
      return str;
    }
    name = JSON.stringify('' + key);
    if (name.match(/^"([a-zA-Z_][a-zA-Z_0-9]*)"$/)) {
      name = name.substr(1, name.length - 2);
      name = ctx.stylize(name, 'name');
    } else {
      name = name.replace(/'/g, "\\'")
                 .replace(/\\"/g, '"')
                 .replace(/(^"|"$)/g, "'");
      name = ctx.stylize(name, 'string');
    }
  }

  return name + ': ' + str;
}


function reduceToSingleString(output, base, braces) {
  var numLinesEst = 0;
  var length = shims.reduce(output, function(prev, cur) {
    numLinesEst++;
    if (cur.indexOf('\n') >= 0) numLinesEst++;
    return prev + cur.replace(/\u001b\[\d\d?m/g, '').length + 1;
  }, 0);

  if (length > 60) {
    return braces[0] +
           (base === '' ? '' : base + '\n ') +
           ' ' +
           output.join(',\n  ') +
           ' ' +
           braces[1];
  }

  return braces[0] + base + ' ' + output.join(', ') + ' ' + braces[1];
}


// NOTE: These type checking functions intentionally don't use `instanceof`
// because it is fragile and can be easily faked with `Object.create()`.
function isArray(ar) {
  return shims.isArray(ar);
}
exports.isArray = isArray;

function isBoolean(arg) {
  return typeof arg === 'boolean';
}
exports.isBoolean = isBoolean;

function isNull(arg) {
  return arg === null;
}
exports.isNull = isNull;

function isNullOrUndefined(arg) {
  return arg == null;
}
exports.isNullOrUndefined = isNullOrUndefined;

function isNumber(arg) {
  return typeof arg === 'number';
}
exports.isNumber = isNumber;

function isString(arg) {
  return typeof arg === 'string';
}
exports.isString = isString;

function isSymbol(arg) {
  return typeof arg === 'symbol';
}
exports.isSymbol = isSymbol;

function isUndefined(arg) {
  return arg === void 0;
}
exports.isUndefined = isUndefined;

function isRegExp(re) {
  return isObject(re) && objectToString(re) === '[object RegExp]';
}
exports.isRegExp = isRegExp;

function isObject(arg) {
  return typeof arg === 'object' && arg;
}
exports.isObject = isObject;

function isDate(d) {
  return isObject(d) && objectToString(d) === '[object Date]';
}
exports.isDate = isDate;

function isError(e) {
  return isObject(e) && objectToString(e) === '[object Error]';
}
exports.isError = isError;

function isFunction(arg) {
  return typeof arg === 'function';
}
exports.isFunction = isFunction;

function isPrimitive(arg) {
  return arg === null ||
         typeof arg === 'boolean' ||
         typeof arg === 'number' ||
         typeof arg === 'string' ||
         typeof arg === 'symbol' ||  // ES6 symbol
         typeof arg === 'undefined';
}
exports.isPrimitive = isPrimitive;

function isBuffer(arg) {
  return arg && typeof arg === 'object'
    && typeof arg.copy === 'function'
    && typeof arg.fill === 'function'
    && typeof arg.binarySlice === 'function'
  ;
}
exports.isBuffer = isBuffer;

function objectToString(o) {
  return Object.prototype.toString.call(o);
}


function pad(n) {
  return n < 10 ? '0' + n.toString(10) : n.toString(10);
}


var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
              'Oct', 'Nov', 'Dec'];

// 26 Feb 16:19:34
function timestamp() {
  var d = new Date();
  var time = [pad(d.getHours()),
              pad(d.getMinutes()),
              pad(d.getSeconds())].join(':');
  return [d.getDate(), months[d.getMonth()], time].join(' ');
}


// log is just a thin wrapper to console.log that prepends a timestamp
exports.log = function() {
  console.log('%s - %s', timestamp(), exports.format.apply(exports, arguments));
};


/**
 * Inherit the prototype methods from one constructor into another.
 *
 * The Function.prototype.inherits from lang.js rewritten as a standalone
 * function (not on Function.prototype). NOTE: If this file is to be loaded
 * during bootstrapping this function needs to be rewritten using some native
 * functions as prototype setup using normal JavaScript does not work as
 * expected during bootstrapping (see mirror.js in r114903).
 *
 * @param {function} ctor Constructor function which needs to inherit the
 *     prototype.
 * @param {function} superCtor Constructor function to inherit prototype from.
 */
exports.inherits = function(ctor, superCtor) {
  ctor.super_ = superCtor;
  ctor.prototype = shims.create(superCtor.prototype, {
    constructor: {
      value: ctor,
      enumerable: false,
      writable: true,
      configurable: true
    }
  });
};

exports._extend = function(origin, add) {
  // Don't do anything if add isn't an object
  if (!add || !isObject(add)) return origin;

  var keys = shims.keys(add);
  var i = keys.length;
  while (i--) {
    origin[keys[i]] = add[keys[i]];
  }
  return origin;
};

function hasOwnProperty(obj, prop) {
  return Object.prototype.hasOwnProperty.call(obj, prop);
}

},{"_shims":9}]},{},[])
;