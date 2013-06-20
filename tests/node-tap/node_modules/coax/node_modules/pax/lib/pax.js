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

