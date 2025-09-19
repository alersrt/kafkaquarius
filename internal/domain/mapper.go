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
	var headers []HeaderString
	for _, hdr := range msg.Headers {
		headers = append(headers, HeaderString{Key: hdr.Key, Value: string(hdr.Value)})
	}

	return &MessageWithStrings{
		Message: msg,
		Key:     string(msg.Key),
		Value:   string(msg.Value),
		Headers: headers,
	}
}

func FromKafkaWithAny(msg *kafka.Message) *MessageWithAny {
	var headers []HeaderAny
	for _, hdr := range msg.Headers {
		headers = append(headers, HeaderAny{Key: hdr.Key, Value: Des(hdr.Value)})
	}

	return &MessageWithAny{
		Message: msg,
		Key:     Des(msg.Key),
		Value:   Des(msg.Value),
		Headers: headers,
	}
}

func ToKafkaWithString(msg *MessageWithStrings) *kafka.Message {
	kMsg := msg.Message
	if kMsg == nil {
		kMsg = &kafka.Message{}
	}

	var headers []kafka.Header
	for _, hdr := range msg.Headers {
		headers = append(headers, kafka.Header{Key: hdr.Key, Value: []byte(hdr.Value)})
	}

	kMsg.Headers = headers
	kMsg.Key = []byte(msg.Key)
	kMsg.Value = []byte(msg.Value)
	return kMsg
}

func ToKafkaWithAny(msg *MessageWithAny) *kafka.Message {
	kMsg := msg.Message
	if kMsg == nil {
		kMsg = &kafka.Message{}
	}

	var headers []kafka.Header
	for _, hdr := range msg.Headers {
		header := kafka.Header{Key: hdr.Key}
		switch val := msg.Key.(type) {
		case string:
			header.Value = []byte(val)
		case []byte:
			header.Value = val
		default:
			header.Value, _ = json.Marshal(msg.Key)
		}
		headers = append(headers, header)
	}
	kMsg.Headers = headers

	switch val := msg.Key.(type) {
	case string:
		kMsg.Key = []byte(val)
	case []byte:
		kMsg.Key = val
	default:
		kMsg.Key, _ = json.Marshal(msg.Key)
	}

	switch val := msg.Value.(type) {
	case string:
		kMsg.Value = []byte(val)
	case []byte:
		kMsg.Value = val
	default:
		kMsg.Value, _ = json.Marshal(msg.Value)
	}
	return kMsg
}
