package internal

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/filter"
	"os"
)

type App struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	filter   *filter.Filter
}

func NewApp(cfg *config.Config) (*App, error) {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.SourceBroker,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	var prod *kafka.Producer
	if cfg.TargetBroker != "" {
		prod, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": cfg.TargetBroker,
			"group.id":          cfg.ConsumerGroup,
		})
		if err != nil {
			return nil, fmt.Errorf("app: new: %v", err)
		}
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	return &App{
		consumer: cons,
		producer: prod,
		filter:   filt,
	}, nil
}
