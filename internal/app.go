package internal

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/filter"
	"os"
)

type App interface {
	Do() error
}

type MigrateApp struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	filter   *filter.Filter
}

func NewApp(cfg *config.Config) (*MigrateApp, error) {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"": cfg.SourceBroker,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"": cfg.SourceBroker,
	})
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	return &MigrateApp{
		consumer: cons,
		producer: prod,
		filter:   filt,
	}, nil
}
