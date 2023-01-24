package js

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"rogchap.com/v8go"
)

func TestSquare(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewV8VM()
	defer vm.Close()

	service := NewService(vm, "square", `function(n) {return n * n;}`)
	assert.Equal(t, "square", service.Name())
	assert.Equal(t, vm, service.Host())

	// Test WithRunner:
	result, err := service.WithRunner(func(irunner Runner) (any, error) {
		runner := irunner.(*V8Runner)
		assert.Nil(t, runner.goContext)
		assert.NotNil(t, runner.ContextOrDefault())
		runner.SetContext(ctx)
		assert.Equal(t, ctx, runner.Context())
		assert.Equal(t, ctx, runner.ContextOrDefault())

		result, err := runner.RunWithV8Args(runner.NewInt(9))
		if err != nil {
			return nil, err
		}
		return result.Integer(), nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 81, result)

	// Test Run:
	result, err = service.Run(ctx, 13)
	assert.NoError(t, err)
	assert.EqualValues(t, 169, result)
}

func TestJSON(t *testing.T) {
	ctx := base.TestCtx(t)
	var pool VMPool
	pool.InitV8(4)
	defer pool.Close()

	service := NewService(&pool, "length", `function(v) {return v.length;}`)

	result, err := service.Run(ctx, []string{"a", "b", "c"})
	if assert.NoError(t, err) {
		assert.EqualValues(t, 3, result)
	}

	result, err = service.Run(ctx, JSONString(`[1,2,3,4]`))
	if assert.NoError(t, err) {
		assert.EqualValues(t, 4, result)
	}
}

func TestCallback(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewV8VM()
	defer vm.Close()

	src := `(function() {
		return hey(1234, "hey you guys!");
	 });`

	var heyParam string

	// A callback function that's callable from JS as hey(num, str)
	hey := func(r *V8Runner, this *v8go.Object, args []*v8go.Value) (result any, err error) {
		assert.Equal(t, len(args), 2)
		assert.Equal(t, int64(1234), args[0].Integer())
		heyParam = args[1].String()
		return 5678, nil
	}

	service := NewCustomService(vm, "callbacks", func(tmpl *BasicTemplate) (Template, error) {
		err := tmpl.SetScript(src)
		tmpl.GlobalCallback("hey", hey)
		return tmpl, err
	})

	result, err := service.Run(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5678, result)
	assert.Equal(t, "hey you guys!", heyParam)
}

// Test conversion of numbers into/out of JavaScript.
func TestNumbers(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewV8VM()
	defer vm.Close()

	service := NewService(vm, "numbers", `function(n, expectedStr) {
		if (typeof(n) != 'number' && typeof(n) != 'bigint') throw "Unexpectedly n is a " + typeof(n);
		let str = n.toString();
		console.info("n=",n,"str=",str);
		if (str != expectedStr) throw "Got " + str + " instead of " + expectedStr;
		return n;
	}`)

	t.Run("integers", func(t *testing.T) {
		testInt := func(n int64) {
			result, err := service.Run(ctx, n, strconv.FormatInt(n, 10))
			if assert.NoError(t, err) {
				assert.EqualValues(t, n, result)
			}
		}

		testInt(-1)
		testInt(0)
		testInt(1)
		testInt(math.MaxInt32)
		testInt(math.MinInt32)
		testInt(math.MaxInt64)
		testInt(math.MinInt64)
		testInt(math.MaxInt64 - 1)
		testInt(math.MinInt64 + 1)
		testInt(kMaxFloat64SafeInt)
		testInt(kMinFloat64SafeInt)
		testInt(kMaxFloat64SafeInt + 1)
		testInt(kMinFloat64SafeInt - 1)
	})

	t.Run("floats", func(t *testing.T) {
		testFloat := func(n float64) {
			result, err := service.Run(ctx, n, strconv.FormatFloat(n, 'f', -1, 64))
			if assert.NoError(t, err) {
				assert.EqualValues(t, n, result)
			}
		}

		testFloat(-1.0)
		testFloat(0.0)
		testFloat(0.001)
		testFloat(1.0)
		testFloat(math.MaxInt32)
		testFloat(math.MinInt32)
		testFloat(math.MaxInt64)
		testFloat(math.MinInt64)
		testFloat(kMaxFloat64SafeInt)
		testFloat(kMinFloat64SafeInt)
		testFloat(kMaxFloat64SafeInt + 1)
		testFloat(kMinFloat64SafeInt - 1)
		testFloat(12345678.12345678)
		testFloat(22.0 / 7.0)
		testFloat(0.1)
	})

	t.Run("json_Number_integer", func(t *testing.T) {
		hugeInt := json.Number("1234567890123456789012345678901234567890")
		result, err := service.Run(ctx, hugeInt, string(hugeInt))
		if assert.NoError(t, err) {
			ibig := new(big.Int)
			ibig, _ = ibig.SetString(string(hugeInt), 10)
			assert.EqualValues(t, ibig, result)
		}
	})

	t.Run("json_Number_float", func(t *testing.T) {
		floatStr := json.Number("1234567890.123")
		result, err := service.Run(ctx, floatStr, string(floatStr))
		if assert.NoError(t, err) {
			assert.EqualValues(t, 1234567890.123, result)
		}
	})
}

// For security purposes, verify that JS APIs to do network or file I/O are not present:
func TestNoIO(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewV8VM()
	defer vm.Close()

	service := NewService(vm, "check", `function() {
		// Ensure that global fns/classes enabling network or file access are missing:
		if (globalThis.fetch !== undefined) throw "fetch exists";
		if (globalThis.XMLHttpRequest !== undefined) throw "XMLHttpRequest exists";
		if (globalThis.File !== undefined) throw "File exists";
		if (globalThis.require !== undefined) throw "require exists";
		// But the following should exist:
		if (globalThis.console === undefined) throw "console is missing!";
		if (globalThis.Math === undefined) throw "Math is missing!";
		if (globalThis.String === undefined) throw "String is missing!";
		if (globalThis.Number === undefined) throw "Number is missing!";
	}`)
	_, err := service.Run(ctx)
	assert.NoError(t, err)
}

// Verify that ECMAScript modules can't be loaded. (The older `require` is checked in TestNoIO.)
func TestNoModules(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewV8VM()
	defer vm.Close()

	src := `import foo from 'foo';
			(function() { });`

	service := NewCustomService(vm, "check", func(tmpl *BasicTemplate) (Template, error) {
		err := tmpl.SetScript(src)
		return tmpl, err
	})
	_, err := service.Run(ctx)
	assert.ErrorContains(t, err, "Cannot use import statement outside a module")
}

func TestTimeout(t *testing.T) {
	vm := NewV8VM()
	defer vm.Close()

	ctx := base.TestCtx(t)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	service := NewService(vm, "forever", `function() { while (true) ; }`)
	start := time.Now()
	_, err := service.Run(ctx)
	assert.Less(t, time.Since(start), 4*time.Second)
	assert.Equal(t, err, context.DeadlineExceeded)
}
