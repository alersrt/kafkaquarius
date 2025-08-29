package internal

import (
	"reflect"
	"testing"
)

func Test_calcPart(t *testing.T) {
	tests := []struct {
		name       string
		threadNo   int
		partsNum   int
		threadsNum int
		exp        []int
	}{
		{"", 0, 5, 3, []int{0, 1}},
		{"", 1, 5, 3, []int{2, 3}},
		{"", 2, 5, 3, []int{4}},
		{"", 0, 5, 5, []int{0}},
		{"", 4, 5, 5, []int{4}},
		{"", 0, 10, 5, []int{0, 1}},
		{"", 4, 10, 5, []int{8, 9}},
		{"", 0, 3, 5, []int{0}},
		{"", 2, 3, 5, []int{2}},
		{"", 3, 3, 5, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			act := calcPart(test.threadNo, test.partsNum, test.threadsNum)
			if !reflect.DeepEqual(test.exp, act) {
				t.Errorf("exp: %+v, act: %v", test.exp, act)
			}
		})
	}
}
