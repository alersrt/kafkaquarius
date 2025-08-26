package domain

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Des(value []byte) any {
	var umars map[string]any
	err := json.Unmarshal(value, &umars)
	if err != nil {
		return string(value)
	} else {
		return umars
	}
}

func FromKafka(msg *kafka.Message) *Message {
	var headers []Header
	for _, hdr := range msg.Headers {
		headers = append(headers, Header{Key: hdr.Key, Value: string(hdr.Value)})
	}

	return &Message{
		Message: msg,
		Key:     string(msg.Key),
		Value:   string(msg.Value),
		Headers: headers,
	}
}
