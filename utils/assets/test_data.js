var hostname = "http://localhost:4985/", db_name = "sync_gateway",
  assert = require("assert"),
  SyncModel = require("./syncModel"),
  sga = require("coax")(hostname)

console.log("test our module loaded")
assert.ok(SyncModel.SyncModelForDatabase, "SyncModelForDatabase API exists")


console.log("test if database is accessible")
var db = sga(db_name)

function getNext(argumentz) {
  var boundNext, args = Array.prototype.slice.call(argumentz),
    next = args[0];
  if (next) {
    boundNext = next.bind.apply(next, args)
  } else {
    boundNext = function(){}
  }
  return boundNext;
}

function setUp() {
  var next = getNext(arguments);
  db.get("_info", function(err, info) {
    // console.log("db info", err, info)
    assert.ok(!err, "error getting database info")
    assert.ok(info.db_name, "info.db_name exists")
    // assert.equal(info.doc_count, 0, "database should be empty")

    console.log("test creating documents on channels")
    db.post("_bulk_docs", {
      docs : [{
        _id : "ace",
        channels : ["xylophone", "yakima", "zoo"]
      }, {
        _id : "booth",
        channels : ["yakima", "zoo"]
      }, {
        _id : "cat",
        channels : ["claws"],
        grant : {
          user : "kitty",
          channels : ["claws", "xylophone"]
        }
      }]
    }, function(err, ok){
      // console.log("posted docs", err, ok)
      assert.ok(!err, "posted docs")
      assert.equal(ok.length, 3, "all saved")
      next()
    })
  })
}

var dbState;
function initData() {
  var next = getNext(arguments);
  console.log("initData",hostname+db_name)
  dbState = SyncModel.SyncModelForDatabase(hostname+db_name)
  // console.log("dbState", dbState)
  var changeHandler = function(change) {
    // console.log("change", change.id)
    assert.notEqual(change.id, "cat", "we called removeListener")
    if (change.id == "booth") {
      // console.log("booth")
      var chan = dbState.channel("yakima")
      // console.log("chan yakima", chan);
      dbState.removeListener("change",changeHandler)
      next()
    }
  }
  dbState.on("change", changeHandler)
}

function runPreview() {
  var next = getNext(arguments);
  console.log("runPreview")
  dbState.once("connected", function() {
    var chan = dbState.channel("yakima")
    console.log("chan.changes", chan.changes)
    assert.ok(chan.changes, "has changes")
    assert.equal(chan.changes[0].id, "booth")
    assert.equal(chan.changes[1].id, "ace")
    var names = dbState.channelNames();
    // console.log("names", names)
    assert.equal(names.length, 4, '"xylophone", "yakima", "zoo", "claws"')
    next()
  })
}

function testAccess() {
  var next = getNext(arguments);
  console.log("testAccess")
  var chan = dbState.channel("xylophone")
  assert.ok(!chan.access, "no access yet")
  next();
};

function testUpdateSyncCode(){
  var next = getNext(arguments);
  console.log("testUpdateSyncCode")
  dbState.setSyncFunction("function(doc){ channel(doc.channels); if (doc.grant) {access(doc.grant.user, doc.grant.channels)} }")
  dbState.once("batch", function(){
    var chan = dbState.channel("xylophone")
    assert.ok(chan.access, "access")
    next();
  })
}

function testRandomDoc() {
  var next = getNext(arguments);
  console.log("testRandomDoc")
  var docid = dbState.randomDocID();
  assert.ok(["ace", "booth", "cat"].indexOf(docid) !== -1, "testRandomDoc")
  next()
}

function testRandomAccessDoc() {
  var next = getNext(arguments);
  var docid = dbState.randomAccessDocID();
  console.log("testRandomAccessDoc")
  assert.equal(docid, "cat", "testRandomAccessDoc")
  next()
}


function testGetDoc() {
  var next = getNext(arguments);
  var docid = dbState.randomAccessDocID();
  console.log("testGetDoc")
  dbState.getDoc(docid, function(doc, deployedSync, previewSync) {
    assert.equal(JSON.stringify(deployedSync.channels), JSON.stringify(previewSync.channels))
    assert.notEqual(JSON.stringify(deployedSync.access), JSON.stringify(previewSync.access), "previewing sync function with access calls, over deployed bucket without")
    next()
  })
}

function testDeploySyncCode(){
  var next = getNext(arguments);
  var newCode = "function(doc){ channel(doc.channels); if (doc.grant) {access(doc.grant.user, doc.grant.channels)} }"
  console.log("testDeploySyncCode")
  dbState.deploySyncFunction(newCode, function(err){
    assert.ok(!err)
    dbState.client.get("_info", function(err, info){
      assert.equal(info.config.sync, newCode)
      next();
    })
  })
}


function newChangesShowUp(){
  var next = getNext(arguments);
  console.log("newChangesShowUp")
  next()
}


setUp(initData, runPreview, testAccess, testUpdateSyncCode, testRandomDoc, testRandomAccessDoc, testGetDoc, testDeploySyncCode, newChangesShowUp)


