//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"errors"
	"fmt"
	"time"
)

// NOTE: This file is adapted from portions of:
// 	https://cs.opensource.google/go/go/%20/refs/tags/go1.22.1:src/time/format.go

var unitMap = map[string]uint64{
	"ns":  uint64(time.Nanosecond),
	"us":  uint64(time.Microsecond),
	"µs":  uint64(time.Microsecond), // U+00B5 = micro symbol
	"μs":  uint64(time.Microsecond), // U+03BC = Greek letter mu
	"ms":  uint64(time.Millisecond),
	"s":   uint64(time.Second),
	"min": uint64(time.Minute),
	"h":   uint64(time.Hour),
	"d":   uint64(time.Hour * 24),
	"w":   uint64(time.Hour * 24 * 7),
	"m":   uint64(time.Hour * 24 * 30),
	"mon": uint64(time.Hour * 24 * 30),
	"y":   uint64(time.Hour * 24 * 365),
}

// ParseLongDuration is a variant of time.ParseDuration that supports longer time units.
// Valid units are "ns", "us" (or "µs"), "ms", "s", "m" (or "min"), "h",
// "d", "w", "m" (or "mon"), "y".
//
// NOTE: "m" is ambiguous. It is assumed to mean "months" unless preceded by a shorter unit;
// so for example "3m" = 3 months, but "2h30m" = 2 hours 30 minutes.
func ParseLongDuration(s string) (time.Duration, error) {
	// [-+]?([0-9]*(\.[0-9]*)?[a-z]+)+
	orig := s
	var d uint64
	neg := false
	small_units := false

	// Consume [-+]?
	if s != "" {
		c := s[0]
		if c == '-' || c == '+' {
			neg = c == '-'
			s = s[1:]
		}
	}
	// Special case: if all that is left is "0", this is zero.
	if s == "0" {
		return 0, nil
	}
	if s == "" {
		return 0, fmt.Errorf("invalid duration %q", orig)
	}
	for s != "" {
		var (
			v, f  uint64      // integers before, after decimal point
			scale float64 = 1 // value = v + f/scale
		)

		var err error

		// The next character must be [0-9.]
		if !(s[0] == '.' || '0' <= s[0] && s[0] <= '9') {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		// Consume [0-9]*
		pl := len(s)
		v, s, err = leadingInt(s)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		pre := pl != len(s) // whether we consumed anything before a period

		// Consume (\.[0-9]*)?
		post := false
		if s != "" && s[0] == '.' {
			s = s[1:]
			pl := len(s)
			f, scale, s = leadingFraction(s)
			post = pl != len(s)
		}
		if !pre && !post {
			// no digits (e.g. ".s" or "-.s")
			return 0, fmt.Errorf("invalid duration %q", orig)
		}

		// Consume unit.
		i := 0
		for ; i < len(s); i++ {
			c := s[i]
			if c == '.' || '0' <= c && c <= '9' {
				break
			}
		}
		if i == 0 {
			return 0, fmt.Errorf("missing unit in duration %q", orig)
		}
		u := s[:i]
		s = s[i:]
		unit, ok := unitMap[u]
		if !ok {
			return 0, fmt.Errorf("unknown unit %q in duration %q", u, orig)
		}

		if !small_units {
			small_units = unit <= uint64(time.Hour)*24
		} else if u == "m" {
			unit = uint64(time.Minute)
		}

		if v > 1<<63/unit {
			// overflow
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		v *= unit
		if f > 0 {
			// float64 is needed to be nanosecond accurate for fractions of hours.
			// v >= 0 && (f*unit/scale) <= 3.6e+12 (ns/h, h is the largest unit)
			v += uint64(float64(f) * (float64(unit) / scale))
			if v > 1<<63 {
				// overflow
				return 0, fmt.Errorf("invalid duration %q", orig)
			}
		}
		d += v
		if d > 1<<63 {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
	}
	if neg {
		return -time.Duration(d), nil
	}
	if d > 1<<63-1 {
		return 0, fmt.Errorf("invalid duration %q", orig)
	}
	return time.Duration(d), nil
}

var errLeadingInt = errors.New("time: bad [0-9]*") // never printed

// leadingInt consumes the leading [0-9]* from s.
func leadingInt[bytes []byte | string](s bytes) (x uint64, rem bytes, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > 1<<63/10 {
			// overflow
			return 0, rem, errLeadingInt
		}
		x = x*10 + uint64(c) - '0'
		if x > 1<<63 {
			// overflow
			return 0, rem, errLeadingInt
		}
	}
	return x, s[i:], nil
}

// leadingFraction consumes the leading [0-9]* from s.
// It is used only for fractions, so does not return an error on overflow,
// it just stops accumulating precision.
func leadingFraction(s string) (x uint64, scale float64, rem string) {
	i := 0
	scale = 1
	overflow := false
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if overflow {
			continue
		}
		if x > (1<<63-1)/10 {
			// It's possible for overflow to give a positive number, so take care.
			overflow = true
			continue
		}
		y := x*10 + uint64(c) - '0'
		if y > 1<<63 {
			overflow = true
			continue
		}
		x = y
		scale *= 10
	}
	return x, scale, s[i:]
}
