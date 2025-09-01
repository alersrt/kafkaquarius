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

type Message struct {
	*kafka.Message
	Key     string
	Value   string
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

func (s Stats) FormattedString() string {
	sentence := `Duration:	{{ .Time }} | Total:	{{ .Total }} | Found:	{{ .Found }} | Proc:	{{ .Proc }} | Errors:	{{ .Errors }} | Threads:	{{ .Threads }} | Offsets:	{{ .Offsets }}`
	templ := template.Must(template.New("stats").Parse(sentence))
	var b strings.Builder
	_ = templ.Execute(&b, s)
	return b.String()
}
