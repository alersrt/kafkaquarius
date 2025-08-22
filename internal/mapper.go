package internal

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func FromKafka(msg *kafka.Message) (map[string]any, error) {
	//var key map[string]any
	//err := json.Unmarshal(msg.Key, &key)
	//if err != nil {
	//	return nil, fmt.Errorf("map: key: %v", err)
	//}
	//var value map[string]any
	//err = json.Unmarshal(msg.Value, &value)
	//if err != nil {
	//	return nil, fmt.Errorf("map: value: %v", err)
	//}
	//headers := make(map[string]any)
	//for _, hdr := range msg.Headers {
	//	hdrVal := make(map[string]any)
	//	err = json.Unmarshal(hdr.Value, &hdrVal)
	//	if err != nil {
	//		return nil, fmt.Errorf("map: headers: %v", err)
	//	}
	//	headers[hdr.Key] = hdrVal
	//}

	//return map[string]any{
	//	VarKey:   key,
	//	VarValue: value,
	//	//VarHeaders:   headers,
	//	VarTimestamp: msg.Timestamp,
	//}, nil

	mars, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("map: from kafka: %v", err)
	}
	var res map[string]any
	err = json.Unmarshal(mars, &res)
	if err != nil {
		return nil, fmt.Errorf("map: from kafka: %v", err)
	}
	return res, nil
}
