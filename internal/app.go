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

	counter := 0
	var errs []error
	for {
		select {
		case <-ctx.Done():
			if err := cons.Close(); err != nil {
				errs = append(errs, err)
				slog.Error(fmt.Sprintf("migrate: %+v", err))
			}
			prod.Close()

			slog.Info(fmt.Sprintf("processed: %d", counter))
			return
		default:
			msg, err := cons.ReadMessage(time.Second)
			counter++
			if err != nil {
				errs = append(errs, err)
				slog.Error(fmt.Sprintf("migrate: %+v", err))
				continue
			}
			ok, err := filt.Eval(msg)
			if err != nil {
				errs = append(errs, err)
				slog.Error(fmt.Sprintf("migrate: %+v", err))
				continue
			}

			msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
			if ok {
				err := prod.Produce(msg, make(chan kafka.Event))
				if err != nil {
					errs = append(errs, err)
					slog.Error(fmt.Sprintf("migrate: %+v", err))
					continue
				}
			}

			if counter%100 == 0 {
				if _, err := cons.Commit(); err != nil {
					errs = append(errs, err)
					slog.Error(fmt.Sprintf("migrate: %+v", err))
					continue
				}
			}

		}
	}
}

func Search(ctx context.Context, cfg *config.Config) {
	return
}
