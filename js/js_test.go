package js

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestSquare(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewVM()
	defer vm.Close()

	service := NewService(vm, "square", `function(n) {return n * n;}`)
	assert.Equal(t, "square", service.Name())
	assert.Equal(t, vm, service.Host())

	// Test WithRunner:
	result, err := service.WithRunner(func(runner *Runner) (any, error) {
		runner.SetContext(ctx)
		result, err := runner.Run(runner.NewInt(9))
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
	pool.Init(4)
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

// Test conversion of numbers into/out of JavaScript.
func TestNumbers(t *testing.T) {
	ctx := base.TestCtx(t)
	vm := NewVM()
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
			assert.EqualValues(t, bigIntWithString(string(hugeInt)), result)
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
