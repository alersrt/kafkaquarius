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

func NewApp(cfg *config.Config) (app *App, err error) {
	app = &App{}

	app.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.SourceBroker,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	if cfg.TargetBroker != "" {
		app.producer, err = kafka.NewProducer(&kafka.ConfigMap{
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
	app.filter, err = filter.NewFilter(string(filtCont))
	if err != nil {
		return nil, fmt.Errorf("app: new: %v", err)
	}

	return
}
