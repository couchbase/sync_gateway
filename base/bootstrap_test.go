package base

import (
	"testing"

	"github.com/imdario/mergo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeStructPointer(t *testing.T) {
	type structPtr struct {
		I *int
		S string
	}
	type wrap struct {
		Ptr *structPtr
	}
	override := wrap{Ptr: &structPtr{nil, "changed"}}

	source := wrap{Ptr: &structPtr{IntPtr(5), "test"}}
	err := mergo.Merge(&source, &override, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)

	require.Nil(t, err)
	assert.Equal(t, "changed", source.Ptr.S)
	assert.Equal(t, IntPtr(5), source.Ptr.I)
}
