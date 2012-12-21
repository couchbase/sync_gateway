// create a user with some channels in the auth db
var coux = require('coux');

function changesReq(feed,since,cb) {
  var timer = setTimeout(function(){
    coux("http://localhost:4984/basecouch", function(err, ok) {
      throw("took too long to return "+feed+
        " with since="+since+". Current seq is "+ok.update_seq)
    });
  }, 1000)
  coux("http://localhost:4984/basecouch/_changes?"+
    "feed="+feed+"&style=all_docs&"+
    "&since="+since+"&filter=basecouch/bychannel&channels="+
    "foo,bar", function(err, ok) {
      if (err) throw(err);
      console.log(ok)
      clearTimeout(timer);
      cb(err, ok)
  });
};

coux.put("http://localhost:4985/GUEST", {
  name: "GUEST", password : "GUEST",
  channels : []
}, function(err, ok) {
  coux.put("http://localhost:4985/example", {
    name: "example", password : "example",
    channels : ["foo","bar", "baz"]
  }, function(err, ok) {

    coux("http://localhost:4984/basecouch/_design/channels", function(err, doc) {
      var mapfun = function(doc) {
            if (doc.tags) sync(doc.tags);
          }.toString();
      if (err) {
        doc = {
          channelmap : mapfun
        };
      } else {
        doc.channelmap = mapfun;
      }
      coux.put("http://localhost:4984/basecouch/_design/channels",
        doc, function(err, ok) {
        if (err) return console.log("err channelmap", err);
        // we are all set up
        var id = Math.random().toString();
        coux.put("http://localhost:4984/basecouch/"+id, {
          tags : ["foo", "bam"]
        }, function(err, ok) {
          // changes feed should have an entry
          changesReq("normal", 0, function(err, ok) {
            if (ok.results[ok.results.length-1].id != id) {
              throw("wanted row for "+id)
            }

            changesReq("normal",ok.last_seq, function() {
              var id = Math.random().toString();
              coux.put("http://localhost:4984/basecouch/"+id, {
                tags : ["foo", "bam"]
              }, function(err, ok) {
                if (err) throw(err);
              })
              changesReq("longpoll",ok.last_seq, function() {

              })
            })
          })
        });
      });
    });
  });
});

// create a document matching a channel
// get changes, ensure you have that document
// get changes since then with regular, should return empty
// get changes with then since and don't get anything until
// you write a matching doc
