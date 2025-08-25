package internal

import (
	"context"
	"encoding/json"
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

	startTs := time.Now()
	totalCnt := 0
	foundCnt := 0
	sentCnt := 0
	errCnt := 0
	defer func() {
		if err := cons.Close(); err != nil {
			slog.Error(fmt.Sprintf("migrate: %+v", err))
		}
		prod.Close()
		slog.Info(fmt.Sprintf("migrate: total: %d", totalCnt))
		slog.Info(fmt.Sprintf("migrate: found: %d", foundCnt))
		slog.Info(fmt.Sprintf("migrate: sent: %d", sentCnt))
		slog.Info(fmt.Sprintf("migrate: errors: %d", errCnt))
		slog.Info(fmt.Sprintf("migrate: duration: %d ms", time.Now().UnixMilli()-startTs.UnixMilli()))
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := cons.ReadMessage(time.Minute)
			if err != nil {
				if err != nil && err.(kafka.Error).IsTimeout() {
					return
				}
				errCnt++
				continue
			}

			ok, err := filt.Eval(msg)
			if err != nil {
				errCnt++
				continue
			}

			if ok {
				foundCnt++
				msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
				err := prod.Produce(msg, nil)
				if err != nil {
					errCnt++
					continue
				}
				sentCnt++
			}
		}
	}
}

func Search(ctx context.Context, cfg *config.Config) {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.SourceBroker,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		slog.Error(fmt.Sprintf("search: %v", err))
		return
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		slog.Error(fmt.Sprintf("search: %v", err))
		return
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		slog.Error(fmt.Sprintf("search: %v", err))
		return
	}

	err = cons.Subscribe(cfg.SourceTopic, nil)
	if err != nil {
		slog.Error(fmt.Sprintf("search: %v", err))
		return
	}

	startTs := time.Now()
	totalCnt := 0
	foundCnt := 0
	errCnt := 0
	fileOffset := int64(0)
	var file *os.File
	if cfg.OutputFile != "" {
		file, err = os.Create(cfg.OutputFile)
		if err != nil {
			slog.Error(fmt.Sprintf("search: %v", err))
			return
		}
		off, _ := file.WriteAt([]byte("["), 0)
		fileOffset += int64(off)
	}

	defer func() {
		if err := cons.Close(); err != nil {
			slog.Error(fmt.Sprintf("search: %+v", err))
		}
		if file != nil {
			_, _ = file.WriteAt([]byte("]"), fileOffset)
			if err := file.Close(); err != nil {
				slog.Error(fmt.Sprintf("search: %+v", err))
			}
		}
		slog.Info(fmt.Sprintf("search: total: %d", totalCnt))
		slog.Info(fmt.Sprintf("search: found: %d", foundCnt))
		slog.Info(fmt.Sprintf("search: errors: %d", errCnt))
		slog.Info(fmt.Sprintf("search: duration: %d ms", time.Now().UnixMilli()-startTs.UnixMilli()))
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := cons.ReadMessage(time.Minute)
			if err != nil {
				if err != nil && err.(kafka.Error).IsTimeout() {
					return
				}
				errCnt++
				continue
			}

			ok, err := filt.Eval(msg)
			if err != nil {
				errCnt++
				continue
			}

			totalCnt++
			if ok {
				foundCnt++
				if file != nil {
					obj, err := json.Marshal(msg)
					if err != nil {
						errCnt++
						continue
					}
					off, _ := file.Write([]byte("\n"))
					fileOffset += int64(off)
					fmt.Printf(string(obj))
					off, _ = file.Write(obj)
					fileOffset += int64(off)
					off, _ = file.Write([]byte(","))
					fileOffset += int64(off)
				}
			}
		}
	}
}
