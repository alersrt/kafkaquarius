package mapper

import (
	"reflect"
	"testing"
)

func TestDes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"string", []byte("string_type"), reflect.TypeFor[string]().String()},
		{"json", []byte("{\"field\":\"value\"}"), reflect.TypeFor[map[string]any]().String()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := Des(test.input)
			if reflect.TypeOf(res).String() != test.expected {
				t.Errorf("wrong type: expected=%s, actual=%s", test.expected, reflect.TypeOf(res).String())
			}
		})
	}
}
