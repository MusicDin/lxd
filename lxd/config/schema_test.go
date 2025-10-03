package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/canonical/lxd/lxd/config"
)

func TestSchema_Defaults(t *testing.T) {
	schema := config.Schema{
		Types: map[string]config.Key{
			"foo": {},
			"bar": {Default: "x"},
		},
	}

	values := map[string]any{"foo": "", "bar": "x"}
	assert.Equal(t, values, schema.Defaults())
}

func TestSchema_Keys(t *testing.T) {
	schema := config.Schema{
		Types: map[string]config.Key{
			"foo": {},
			"bar": {Default: "x"},
		},
	}

	keys := []string{"bar", "foo"}
	assert.Equal(t, keys, schema.Keys())
}
