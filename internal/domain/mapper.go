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

func FromKafkaWithStrings(msg *kafka.Message) *MessageWithStrings {
	var headers []Header
	for _, hdr := range msg.Headers {
		headers = append(headers, Header{Key: hdr.Key, Value: string(hdr.Value)})
	}

	return &MessageWithStrings{
		Message: msg,
		Key:     string(msg.Key),
		Value:   string(msg.Value),
		Headers: headers,
	}
}

func FromKafkaWithAny(msg *kafka.Message) *MessageWithAny {
	var headers []Header
	for _, hdr := range msg.Headers {
		headers = append(headers, Header{Key: hdr.Key, Value: string(hdr.Value)})
	}

	return &MessageWithAny{
		Message: msg,
		Key:     Des(msg.Key),
		Value:   Des(msg.Value),
		Headers: headers,
	}
}
