Fast Statistics
===============

A NodeJS library to do statistical analysis of numeric datasets.

introduction
------------

When doing statistical analysis of data, the most common usage pattern is to run multiple statistical methods on the same set
of data.  Some of these methods use others.  For example, to calculate the standard deviation of a dataset, we first need the
mean.

Additionally, some methods can be calculated quickly as data is inserted, thereby reducing the number of loops required to
run through the data during processing.

Fast stats maintains a running cache of several summary values as data is inserted making final calculation very fast.  It
trades off a small amount of additional memory usage for a large reduction in execution time.

downsides
---------

The downside of how fast stats operates is that if your datapoints are too large, it may result in numeric overflow causing
incorrect results.  Fast stats does not attempt to detect or correct for this.

synopsis
--------

```javascript
var Stats = require('fast-stats').Stats;

var s = new Stats(1, 2, 3, 10, 8, 4, 3);
console.log(s.amean().toFixed(2));
// 4.43
```

installation
------------

    $ npm install fast-stats

API
---

fast-stats is completely synchronous.  There are no blocking methods and consequently no callbacks involved.  All
runtime calls are executed in-memory and are fast.

### Getting data in and out

#### Initialising and adding data

The `Stats` constructor looks a lot like an array in the way you add and remove data to its ends, however there is
no direct access to individual elements.  The constructor takes in multiple values or a single list of values.  All
values must be numbers and behaviour is undefined if they are not.

Additionally, the `push()` method may take in a list of values that will be added to the end of the current list and
the `unshift()` method may take in a list of values that will be added to the beginning of the list.

The following are equivalent.

```javascript
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
```

Note that we use the `toFixed()` method of the Number class when comparing numbers.  Remember that even if you
pass in integers, values like the arithmetic mean, standard deviation and median can sometimes be floating point
numbers, and two floating point numbers may not necessarily be equal to the last decimal point.  The `toFixed()`
method is useful to restrict how precise we want our comparison to be.  Be aware that it returns a string though.

`fast-stats` does not use the `toFixed()` method internally.

The `push()` and `unshift()` methods return the new length of the object.

#### Removing data

If you need to remove data from a `Stats` object, use the `pop()` and `shift()` methods.  Their semantics are the
same as the `pop()` and `shift()` methods of Arrays.

```javascript
var a = s1.pop();
assert.equal(a, 3);

var b = s2.shift();
assert.equal(b, 1);

assert.equal(s1.length, 6);
assert.equal(s2.length, 6);
assert.ok(s1.amean() < s2.amean());
```

#### Clearing all data

The `reset()` method clears out all data.

```javascript
s4.reset();
assert.equal(s4.length, 0);
```

The `reset()` method returns a reference to the object, so you can chain methods.

#### Making a copy

The `copy()` method returns a copy of the current Stats object.

```javascript
s4 = s3.copy();

assert.equal(s1.length, s2.length);
```

### Summaries & Averages

The term _Average_ is overloaded in Statistics.  It relates to a summary of a data set, but says nothing about how
we arrived at that summary.  There are many ways to summarise data, including the arithmetic mean, geometric mean,
harmonic mean, median, mode and more.  `fast-stats` implements the Arithmetic Mean, the Geometric Mean and the Median.
It also implements a percentile method to get at any percentile of the data.

#### Arithmetic Mean

The arithmetic mean is calculated as the sum of all data points divided by the number of data points.  This is useful
for data sets that are fairly uniform, following a linear or binomial distribution.  Use the `amean()` method or the `μ()`
method to get at it:

```javascript
var a = s1.amean();
assert.equal(a.toFixed(2), "4.67");   // remember we popped out the last item of `s1` above.
```

#### Geometric Mean

The arithmetic mean is the `n`th root of the product of all data points where n is the number of data points. This is useful
for data sets that follow an exponential or log-normal distribution.  Use the `gmean()` method to get at it:

```javascript
var a = s1.gmean();
assert.equal(a.toFixed(2), "3.53");
```

#### Median

The median is the middle point of the dataset when sorted in ascending order.  This is useful if your dataset has a lot of
outliers and noise that would not normally be found in a complete population.  Use the `median()` method to get at it:

```javascript
var a = s1.median();
assert.equal(a.toFixed(2), "3.50");
```

If your data set contains an odd number of points, the median will be the middle point.  If it contains an even number of
points, then the median will be the arithmetic mean of the two middle points.

#### Any Percentile

You can also get at any percentile value within the data.  Use the `percentile()` method to get at this data.  The
`percentile()` method takes in a single argument.  This is a number between 0 and 100 (both inclusive) that specifies
which percentile point you want.

```javascript
var p95 = s1.percentile(95);
var m = s1.percentile(50);
var q1 = s1.percentile(25);

assert.equal(p95.toFixed(2), "10.00");
assert.equal(m.toFixed(2), "3.50");
assert.equal(q1.toFixed(2), "2.50");
```

Passing in 50 as an argument will return the median, while 25 and 75 will return the first and third quartiles respectively.
These three special values may be arithmetic means of two other values within the set.  All other arguments will return a
number from the data set.

#### Range

The `range()` method tells you the minimum and maximum values of your data set.  It returns an array of two values.  The
first is the lower bound and the second is the upper bound.

```javascript
var r = s1.range();

assert.equal(r.length, 2);
assert.equal(r[0], 1);
assert.equal(r[1], 10);
```


### Data Accuracy

There are various statistical values that tell you how accurate or uniform your data is.  `fast-stats` implements
the Arithmetic Standard Deviation, Geometric Standard Deviation and 95% Confidence Interval Margin of Error.

#### Arithmetic Standard Deviation

Also commonly just called the Standard Deviation, with the symbol σ.  This tells you the spread of your data if
it follows a normal (or close to normal) distribution, ie, the bell curve.  `fast-stats` is really fast at
calculating the standard deviation of a dataset.  Use the `stddev()` method or the `σ()` method to get at it.

```javascript
var sd = s1.σ();

assert.equal(sd.toFixed(2), '3.25');
```

The arithmetic standard deviation is used in conjunction with the arithmetic mean to tell you the spread of your
dataset: `[amean-stddev, amean+stddev]`.  Note that you could also use 2 or 3 standard deviations for different
spreads.

#### Geometric Standard Deviation

The geometric mean tells you the spread of your data if it follows a log-normal or exponential distribution.
Use the `gstddev()` method to get at it.

```javascript
var gsd = s1.gstddev();

assert.equal(gsd.toFixed(2), '2.20');
```

The geometric standard deviation is used in conjunction with the geometric mean to tell you the spread of your
dataset: `[gmean/gstddev, gmean*gstddev]`.  Note that this range is not symmetric around the geometric mean.

#### 95% Confidence Margin of Error

The Margin of Error value tells you the range within which the real arithmetic mean of the population is likely to
be with 95% confidence.  Use the `moe()` method to get at it.

```javascript
var moe = s1.moe();

assert.equal(moe.toFixed(2), '2.60');
```

This value suggests that we are 95% certain that the real mean of the population is within 2.60 of the calculated 
arithmetic mean of 4.67.  We could use this to find out the percent error in our sample.  In this case there is a
55.71% error.

The margin of error is inversely proportional to the square root of the number of data points, so increasing the
size of your sample will reduce the margin of error.  It is good to strive for a margin of error of less than 5%.

### Data filtering

When dealing with statistical samples, it may be necessary to filter the dataset to get rid of outliers.  Sometimes
an outlier is fairly obvious, and you can specify an upper and lower limit for it.   At other times, outliers are
only apparent when looking at the rest of the dataset.  Inter-Quartile-Range filtering is useful to filter out these
kinds of data sets.

#### Band-pass filtering

The `band_pass()` filter method returns a new `Stats` object with all its data points within the specified range.
This method takes in three arguments.  The first is the lower bound of the range, the second is the upper bound
of the range.  Both these arguments are required.

The third argument specifies whether the range is open or closed.  An open range does not include the upper and
lower bounds while a closed range includes them.  If not specified (or set to `false`), the range is closed.  If
set to `true` the range is open.

```javascript
var s5 = s1.band_pass(3, 8);
var r = s5.range();

assert.equal(r[0], 3);
assert.equal(r[1], 8);

s5 = s1.band_pass(3, 8, true);
r = s5.range();

assert.equal(r[0], 4);
assert.equal(r[1], 4);
```

Band pass filtering should be used if the range for your data is rigid and never changes.

#### IQR Filtering

IQR, or Inter Quartile Range filtering filters data based on the spread of the data.  It is much more adaptive to
changes in data ranges.  Use the `iqr()` method to IQR filter a dataset.  The `iqr()` method does not accept
any arguments.


```javascript
var s6 = s1.iqr();
r = s6.range();

assert.equal(r[0], 1);
assert.equal(r[1], 10);
```

In some cases, IQR filtering may not filter out anything.  This can happen if the acceptable range is wider than
the bounds of your dataset.


References
----------

Wikipedia is a great place to get information about Statistical functions.

Copyright
---------

`fast-stats` is Copyright 2011 Philip Tellis <philip@bluesmoon.info> and the latest version of the code is
available at https://github.com/bluesmoon/node-faststats

License
-------

Apache 2.0.  See the [LICENSE](https://github.com/bluesmoon/node-faststats/blob/master/LICENSE) file for details.
