package consumer

import (
	"testing"
	"time"
)

func TestParallelConsumer_Do(t *testing.T) {
	testedUnit := &ParallelConsumer{
		threadsNum: 10,
		sinceTime:  time.Time{},
		toTime:     time.Time{},
		offsets:    make(map[int32]int64),
		consumers:  nil,
		topic:      "test",
	}

	if testedUnit != nil {
	}
}
