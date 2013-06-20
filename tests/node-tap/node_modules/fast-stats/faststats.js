/*
Note that if your data is too large, there _will_ be overflow.
*/


function asc(a, b) { return a-b; }

function Stats() {
	var a = arguments;
	if(a[0] instanceof Array)
		a=a[0];
	this.reset();
	if(a)
		this.push.apply(this, a);

	return this;
}

Stats.prototype = {
	reset: function() {
		this.data = [];
		this.length = 0;
	
		this.sum = 0;
		this.sum_of_squares = 0;
		this.sum_of_logs = 0;
		this.sum_of_square_of_logs = 0;
		this.max = this.min = null;
	
		this._reset_cache();

		return this;
	},

	_reset_cache: function() {
		this._amean = null;
		this._gmean = null;
		this._stddev = null;
		this._gstddev = null;
		this._moe = null;
		this._data_sorted = null;
	},

	_add_cache: function(a) {
		this.sum += a;
		this.sum_of_squares += a*a;
		this.sum_of_logs += Math.log(a);
		this.sum_of_square_of_logs += Math.pow(Math.log(a), 2);
		this.length++;

		if(this.max === null || this.max < a)
			this.max = a;
		if(this.min === null || this.min > a)
			this.min = a;

		this._reset_cache();
	},

	_del_cache: function(a) {
		this.sum -= a;
		this.sum_of_squares -= a*a;
		this.sum_of_logs -= Math.log(a);
		this.sum_of_square_of_logs -= Math.pow(Math.log(a), 2);
		this.length--;

		if(this.length === 0) {
			this.max = this.min = null;
		}
		else if(this.max === a || this.min === a) {
			var i = this.length-1;
			this.max = this.min = this.data[i--];
			while(i--) {
				if(this.max < this.data[i])
					this.max = this.data[i];
				if(this.min > this.data[i])
					this.min = this.data[i];
			}
		}

		this._reset_cache();
	},

	push: function() {
		var i, a;
		for(i=0; i<arguments.length; i++) {
			a = arguments[i];
			this.data.push(a);
			this._add_cache(a);
		}

		return this.length;
	},

	pop: function() {
		if(this.length === 0)
			return undefined;

		var a = this.data.pop();
		this._del_cache(a);

		return a;
	},

	unshift: function() {
		var i=arguments.length, a;
		while(i--) {
			a = arguments[i];
			this.data.unshift(a);
			this._add_cache(a);
		}

		return this.length;
	},

	shift: function() {
		if(this.length === 0)
			return undefined;

		var a = this.data.shift();
		this._del_cache(a);

		return a;
	},

	amean: function() {
		if(this.length === 0)
			return NaN;
		if(this._amean === null)
			this._amean = this.sum/this.length;

		return this._amean;
	},

	gmean: function() {
		if(this.length === 0)
			return NaN;
		if(this._gmean === null)
			this._gmean = Math.exp(this.sum_of_logs/this.length);

		return this._gmean;
	},

	stddev: function() {
		if(this.length === 0)
			return NaN;
		if(this._stddev === null)
			this._stddev = Math.sqrt(this.length * this.sum_of_squares - this.sum*this.sum)/this.length;

		return this._stddev;
	},

	gstddev: function() {
		if(this.length === 0)
			return NaN;
		if(this._gstddev === null)
			this._gstddev = Math.exp(Math.sqrt(this.length * this.sum_of_square_of_logs - this.sum_of_logs*this.sum_of_logs)/this.length);

		return this._gstddev;
	},

	moe: function() {
		if(this.length === 0)
			return NaN;
		// see http://en.wikipedia.org/wiki/Standard_error_%28statistics%29
		if(this._moe === null)
			this._moe = 1.96*this.stddev()/Math.sqrt(this.length);

		return this._moe;
	},

	range: function() {
		if(this.length === 0)
			return [NaN, NaN];
		return [this.min, this.max];
	},

	percentile: function(p) {
		if(this.length === 0)
			return NaN;
		if(this._data_sorted === null)
			this._data_sorted = this.data.sort(asc);

		if(p <=  0)
			return this._data_sorted[0];
		if(p == 25)
			return (this._data_sorted[Math.floor((this.length-1)*0.25)] + this._data_sorted[Math.ceil((this.length-1)*0.25)])/2;
		if(p == 50)
			return this.median();
		if(p == 75)
			return (this._data_sorted[Math.floor((this.length-1)*0.75)] + this._data_sorted[Math.ceil((this.length-1)*0.75)])/2;
		if(p >= 100)
			return this._data_sorted[this.length-1];

		return this._data_sorted[Math.floor(this.length*p/100)];
	},

	median: function() {
		if(this.length === 0)
			return NaN;
		if(this._data_sorted === null)
			this._data_sorted = this.data.sort(asc);

		return (this._data_sorted[Math.floor((this.length-1)/2)] + this._data_sorted[Math.ceil((this.length-1)/2)])/2;
	},

	iqr: function() {
		var q1, q3, fw;

		q1 = this.percentile(25);
		q3 = this.percentile(75);
	
		fw = (q3-q1)*1.5;
	
		return this.band_pass(q1-fw, q3+fw, true);
	},

	band_pass: function(low, high, open) {
		var i, b=new Stats();

		if(this.length === 0)
			return new Stats();

		if(this._data_sorted === null)
			this._data_sorted = this.data.sort(asc);

		for(i=0; i<this.length && (this._data_sorted[i] < high || (!open && this._data_sorted[i] === high)); i++) {
			if(this._data_sorted[i] > low || (!open && this._data_sorted[i] === low)) {
				b.push(this._data_sorted[i]);
			}
		}

		return b;
	},

	copy: function() {
		return new Stats(this.data);
	}
};

Stats.prototype.σ=Stats.prototype.stddev;
Stats.prototype.μ=Stats.prototype.amean;


exports.Stats = Stats;

if(process.argv[1] && process.argv[1].match(__filename)) {
	var s = new Stats(1, 2, 3);
	var l = process.argv.slice(2);
	if(!l.length) l = [10, 11, 15, 8, 13, 12, 19, 32, 17, 16];
	l.forEach(function(e, i, a) { a[i] = parseFloat(e, 10); });
	Stats.prototype.push.apply(s, l);
	console.log(s.data);
	console.log(s.amean().toFixed(2), s.μ().toFixed(2), s.stddev().toFixed(2), s.σ().toFixed(2), s.gmean().toFixed(2), s.median().toFixed(2), s.moe().toFixed(2));
}
