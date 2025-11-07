package cel

import (
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestFilter(t *testing.T) {
	filterContent := `unmarshal(self.Value).some == 0
&& self.Headers.exists(h, h.Value in [b'value', b'wrong'])
&& self.Headers.size() != 0
&& string(self.Key).matches(".*test_key.*")
&& uuid.v7() != ''
&& self.Timestamp > timestamp('1970-01-01T00:00:00.000Z')
`
	testedUnit, err := NewCel(filterContent)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	msg := &kafka.Message{
		Key:   []byte("{\"kf\":\"test_key\"}"),
		Value: []byte("{\"some\":0}"),
		Headers: []kafka.Header{{
			Key:   "key1",
			Value: []byte("value"),
		}},
		Timestamp: time.Now(),
	}
	ok, err := testedUnit.Eval(msg, reflect.TypeFor[bool]())
	if err != nil {
		t.Errorf("%+v", err)
	}
	if c, r := ok.(bool); !c && !r {
		t.Errorf("expected true")
	}
}

func BenchmarkFilter_Eval(b *testing.B) {
	filterContent := `unmarshal(self.Value).some == 0
&& self.Headers.exists(h, h.Value in [b'value', b'wrong'])
&& self.Headers.size() != 0
&& string(self.Key).matches(".*test_key.*")
&& uuid.v7() != ''
&& self.Timestamp > timestamp('1970-01-01T00:00:00.000Z')
`
	testedUnit, _ := NewCel(filterContent)

	msg := &kafka.Message{
		Key:   []byte("{\"kf\":\"test_key\"}"),
		Value: []byte("{\"some\":0}"),
		Headers: []kafka.Header{{
			Key:   "key1",
			Value: []byte("value"),
		}},
		Timestamp: time.Now(),
	}

	for i := 0; i < b.N; i++ {
		_, _ = testedUnit.Eval(msg, reflect.TypeFor[bool]())
	}
}
