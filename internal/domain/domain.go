package domain

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
	"text/template"
	"time"
)

type Header struct {
	Key   string
	Value string
}

type MessageWithStrings struct {
	*kafka.Message
	Key     string
	Value   string
	Headers []Header
}

type MessageWithAny struct {
	*kafka.Message
	Key     any
	Value   any
	Headers []Header
}

type Stats struct {
	Total   uint64
	Found   uint64
	Proc    uint64
	Errors  uint64
	Time    time.Duration
	Threads int32
	Offsets map[int32]int64
}

func (s Stats) FormattedString(sentence string) string {
	templ := template.Must(template.New("stats").Parse(sentence))
	var b strings.Builder
	err := templ.Execute(&b, s)
	if err != nil {
		return ""
	}
	return b.String()
}
