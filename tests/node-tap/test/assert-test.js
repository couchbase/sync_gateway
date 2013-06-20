var test = require("tap").test,
  coax = require("coax");

test("reachable", function(t){
  coax("http://localhost:4984/default", function(err, info) {
    t.assert(!err, "no error")
    t.equal(info.db_name, "default", "correct db name")
    t.end()
  })
})


