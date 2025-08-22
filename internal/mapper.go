package internal

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func JsonDes(value []byte) (map[string]any, error) {
	var res map[string]any
	err := json.Unmarshal(value, &res)
	if err != nil {
		return nil, fmt.Errorf("map: json des: %v", err)
	}
	return res, nil
}

func StringDes(value []byte) string {
	return string(value)
}

func FromKafka(msg *kafka.Message) (map[string]any, error) {
	key := StringDes(msg.Key)
	value, err := JsonDes(msg.Value)
	if err != nil {
		return nil, fmt.Errorf("map: from kafka: %v", err)
	}
	headers := make(map[string]any, len(msg.Headers))
	for _, hdr := range msg.Headers {
		headers[hdr.Key] = StringDes(hdr.Value)
	}

	return map[string]any{
		VarKey:       key,
		VarValue:     value,
		VarHeaders:   headers,
		VarTimestamp: msg.Timestamp,
	}, nil
}
