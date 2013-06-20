var assert = require('assert');

var Stats = require('fast-stats').Stats;
Array = require('gauss').Vector;

var a = [], b = new Stats();

for(var i=0; i<100001; i++) {
	var n = Math.round(Math.random()*10000);

	a.push(n);
	b.push(n);
}

var amn, bmn;

console.time("gauss-mean");
amn = a.mean();
console.timeEnd("gauss-mean");
console.time("fast-mean");
bmn = b.amean();
console.timeEnd("fast-mean");
assert.equal(amn.toFixed(2), bmn.toFixed(2));

console.time("gauss-median");
amn = a.median();
console.timeEnd("gauss-median");
console.time("fast-median");
bmn = b.median();
console.timeEnd("fast-median");
assert.equal(amn.toFixed(2), bmn.toFixed(2));

console.time("gauss-stddev");
amn = a.stdev();
console.timeEnd("gauss-stddev");
console.time("fast-stddev");
bmn = b.stddev();
console.timeEnd("fast-stddev");
assert.equal(amn.toFixed(2), bmn.toFixed(2));
