package internal

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	VarKey       = "Key"
	VarValue     = "Value"
	VarTimestamp = "Timestamp"
	VarHeaders   = "Headers"
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

func FromKafka(msg *kafka.Message) map[string]any {
	headers := make(map[string]any, len(msg.Headers))
	for _, hdr := range msg.Headers {
		headers[hdr.Key] = Des(hdr.Value)
	}

	return map[string]any{
		VarKey:       Des(msg.Key),
		VarValue:     Des(msg.Value),
		VarHeaders:   headers,
		VarTimestamp: timestamppb.New(msg.Timestamp),
	}
}
