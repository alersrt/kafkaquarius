package internal

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/filter"
	"log/slog"
	"os"
	"time"
)

func Migrate(ctx context.Context, cfg *config.Config) {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.SourceBroker,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		slog.Error(fmt.Sprintf("migrate: %v", err))
		return
	}

	var prod *kafka.Producer
	if cfg.TargetBroker != "" {
		prod, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": cfg.TargetBroker,
			"group.id":          cfg.ConsumerGroup,
		})
		if err != nil {
			slog.Error(fmt.Sprintf("migrate: %v", err))
			return
		}
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		slog.Error(fmt.Sprintf("migrate: %v", err))
		return
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		slog.Error(fmt.Sprintf("migrate: %v", err))
		return
	}

	err = cons.Subscribe(cfg.SourceTopic, nil)
	if err != nil {
		slog.Error(fmt.Sprintf("migrate: %v", err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			cons.Close()
			prod.Close()
			return
		default:
			msg, err := cons.ReadMessage(time.Second)
			if err != nil {
				slog.Error(fmt.Sprintf("migrate: %+v", err))
				continue
			}
			ok, err := filt.Eval(msg)
			if err != nil {
				slog.Error(fmt.Sprintf("migrate: %+v", err))
				continue
			}

			if ok {
				prod.Produce(msg, make(chan kafka.Event))
			}

			cons.Commit()
		}
	}
}

func Search(cfg *config.Config) error {
	return nil
}
