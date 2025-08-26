package domain

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
