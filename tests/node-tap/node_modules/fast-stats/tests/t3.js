var assert = require('assert'),
    Stats = require('fast-stats').Stats;

var s1, s2, s3, s4;
s1 = new Stats(1, 2, 3, 10, 8, 4, 3);

s2 = new Stats([1, 2, 3, 10, 8, 4, 3]);

s3 = new Stats();
s3.push(1, 2, 3, 10, 8, 4, 3);

s4 = new Stats();
s4.unshift(1, 2, 3, 10, 8, 4);
s4.push(3);

assert.equal(s1.amean().toFixed(2), s2.amean().toFixed(2));
assert.equal(s1.amean().toFixed(2), s3.amean().toFixed(2));
assert.equal(s1.amean().toFixed(2), s4.amean().toFixed(2));

var a = s1.pop();
assert.equal(a, 3);

var b = s2.shift();
assert.equal(b, 1);

assert.equal(s1.length, 6);
assert.equal(s2.length, 6);
assert.ok(s1.amean() < s2.amean());


s4.reset();
assert.equal(s4.length, 0);

s4 = s3.copy();

assert.equal(s1.length, s2.length);

var a = s1.amean();
assert.equal(a.toFixed(2), "4.67");   // remember we popped out the last item of `s1` above.

var a = s1.gmean();
assert.equal(a.toFixed(2), "3.53");

var a = s1.median();
assert.equal(a.toFixed(2), "3.50");

var p95 = s1.percentile(95);
var m = s1.percentile(50);
var q1 = s1.percentile(25);

assert.equal(p95.toFixed(2), "10.00");
assert.equal(m.toFixed(2), "3.50");
assert.equal(q1.toFixed(2), "2.50");

var r = s1.range();

assert.equal(r.length, 2);
assert.equal(r[0], 1);
assert.equal(r[1], 10);

var sd = s1.σ();

assert.equal(sd.toFixed(2), '3.25');

var gsd = s1.gstddev();

assert.equal(gsd.toFixed(2), '2.20');

var moe = s1.moe();

assert.equal(moe.toFixed(2), '2.60');

var s5 = s1.band_pass(3, 8);
var r = s5.range();

assert.equal(r[0], 3);
assert.equal(r[1], 8);

s5 = s1.band_pass(3, 8, true);
r = s5.range();

assert.equal(r[0], 4);
assert.equal(r[1], 4);

var s6 = s1.iqr();
r = s6.range();

assert.equal(r[0], 1);
assert.equal(r[1], 10);
