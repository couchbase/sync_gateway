var Stats = require('fast-stats').Stats;

var a = new Stats();
for(var i=0; i<11; i++) {
	var n = Math.round(Math.random()*10000);

	a.push(n);
}

var b = a.copy();
a.push(15);

console.log(a.data, b.data);
