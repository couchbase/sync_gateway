var //config = require("./config.js"),
  async = require("async"),
  coax = require("coax"),
  test = require("tap").test;


  var httpAgent = require("http").globalAgent;
  httpAgent.maxSockets = 1000000;

var db = coax("http://localhost:4984/default")


test("sync gateway database is reachable", function(t){
  db(function(err, info){
    t.assert(!err, "no errors");
    t.assert(info.db_name, "is a database")
    t.end()
  })
})

test("docs are seen on the changes feed of all channels", function(t) {
  var rev, timer, chans = db.channels(["ok","abc","empty"])

  chans.on("json", function(json){
    clearTimeout(timer)
    timer = setTimeout(function(){
      chans.emit("quiet", json)
    },100)
  })

  db.forceSave({_id:"test-doc-b",channels:["ok","abc"]}, function(err, ok){
      t.assert(!err, "doc-saved")
      db.forceSave({_id:"test-doc-b",channels:["ok","abc"]}, function(err, ok){
          t.assert(!err, "doc-saved")
      })
  })

  chans.once("quiet", function(json){
    console.log("begin watch and save", json.seq)
    t.assert(json.seq, "has a sequence")

    var chans2 = db.channels(["ok","abc","empty"],{since:json.seq})

    chans2.on("json", function (change) {
      // clear so use the
      if (change.id == "test-doc" && change.changes[0].rev == rev) {
        t.assert(change.seq, "has a sequence")
        console.log("saved seq", change.seq)
        var chs = parseSequence(change.seq);
        t.assert(chs["abc"],"on abc channel");
        t.assert(chs["ok"],"on ok channel");
        chans2.removeAllListeners("json")
        t.end()
      }
    })
    console.log("will put")
    db.forceSave({_id:"test-doc",channels:["ok","abc"]}, function(err, ok){
      rev = ok.rev
      console.log("did put", err, ok)
      t.assert(!err,"no err on saved doc")
      // if (err) t.end()
    })
  })

});

test("what happens after a channel has a doc in it", function(t){
  var chans3 = db.channels(["ok","abc","empty"])

  chans3.once("quiet", function(json) {
    t.assert(json.seq, "has a seq")
    t.end()
  })
  db.forceSave({_id:"test-doc-b", channels : ['empty']}, function(err, ok){
    t.assert(!err, "doc saved")


  })
})

function parseSequence(seq) {
  // parse sequence number
  var chs = {}, seqchans = seq.split(',');
  for (var i = seqchans.length - 1; i >= 0; i--) {
    var kv = seqchans[i].split(':')
    chs[kv[0]] = parseInt(kv[1])
  };
  return chs;
}

test("docs aren't on channels they shouldn't be on", function(t) {
  var chans = db.channels(["ok","abc","xyz"])
  // when replication is complete, request changes and verify data stucture
  db.forceSave({_id:"test-doc", channels:["abc"]}, function(err, ok0){
    db.forceSave({_id:"test-doc", channels:["abc"]}, function(err, ok1){
      db.forceSave({_id:"test-doc", channels:["abc"]}, function(err, ok){
        t.assert(ok.id, "saved")
        // console.log()
        chans.on("json", function (change){
          if (change.id == ok.id && change.changes[0].rev == ok.rev) {
            // console.log("seq",change.seq)
            t.assert(change.seq, "has a sequence")

            var chs = parseSequence(change.seq);
            t.equals(chs["abc"]-chs["ok"], 2, "stopped appearing on old channel")
            t.end()
          }
        });
      })
    })
  })
})



test("invisible on a channel without the doc", function(t) {
  // when replication is complete, request changes and verify data stucture
  var chans = db.channels(["abc"])
  var rare = db.channels(["rare"])

  db.forceSave({_id:"rare-doc", channels:["rare"]}, function(err, ok){
    rare.on("json", function(change){
      if (change.id == ok.id && change.changes[0].rev == ok.rev) {
        t.assert(change.seq, "has a sequence")
        t.end()
      }
    })
    chans.on("json", function(change){
      if (change.id == ok.id && change.changes[0].rev == ok.rev) {
        t.assert(false, "should not be on this channel")
        t.end()
      }
    })
  })
})

test("verify filtered data", function(t) {
  // when replication is complete, request changes and verify data stucture
  t.end()
  process.exit()
})
