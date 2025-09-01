package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/consumer"
	"kafkaquarius/internal/domain"
	"kafkaquarius/internal/filter"
	"os"
	"sync/atomic"
	"time"
)

func Execute(ctx context.Context, cmd string, cfg *config.Config) (*domain.Stats, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	consCfg := kafka.ConfigMap{
		"bootstrap.servers":  cfg.SourceBroker,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}
	pCons, err := consumer.NewParallelConsumer(cfg.ThreadsNumber, cfg.SinceTime, cfg.ToTime, cfg.SourceTopic, consCfg)
	if err != nil {
		return nil, err
	}
	defer pCons.Close()

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return nil, err
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return nil, err
	}

	startTs := time.Now()
	var totalCnt atomic.Uint64
	var foundCnt atomic.Uint64
	var procCnt atomic.Uint64
	var errCnt atomic.Uint64

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("Duration:\t%s\r", time.Since(startTs).Truncate(time.Second))
			}
		}
	}()

	switch cmd {
	case config.CmdMigrate:
		var prod *kafka.Producer
		prod, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": cfg.TargetBroker,
		})
		defer prod.Close()
		if err != nil {
			return nil, err
		}
		err = pCons.Do(ctx,
			func(err error) {
				errCnt.Add(1)
			},
			func(msg *kafka.Message) {
				totalCnt.Add(1)
			},
			func(msg *kafka.Message) {
				if ok, _ := filt.Eval(msg); ok {
					msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
					err := prod.Produce(msg, nil)
					if err != nil {
						errCnt.Add(1)
					} else {
						procCnt.Add(1)
					}
				}
			},
		)
	case config.CmdSearch:
		var file *os.File
		if cfg.OutputFile != "" {
			file, err = os.Create(cfg.OutputFile)
			if err != nil {
				return nil, err
			}
			defer func() {
				_ = file.Close()
			}()
		}

		write := func(msg *kafka.Message) error {
			if file == nil {
				return nil
			}
			bytes, err := json.Marshal(domain.FromKafka(msg))
			if err != nil {
				return err
			}
			_, err = file.Write(bytes)
			if err != nil {
				return err
			}
			_, err = file.WriteString("\n")
			if err != nil {
				return err
			}
			return nil
		}

		err = pCons.Do(ctx,
			func(err error) {
				errCnt.Add(1)
			},
			func(msg *kafka.Message) {
				totalCnt.Add(1)
			},
			func(msg *kafka.Message) {
				if ok, _ := filt.Eval(msg); ok {
					err := write(msg)
					if err != nil {
						errCnt.Add(1)
					} else {
						procCnt.Add(1)
					}
				}
			},
		)
	}

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, ctx.Err()) {
		err = errors.Join(err, cause)
	}

	return &domain.Stats{
		Total:  totalCnt.Load(),
		Found:  foundCnt.Load(),
		Proc:   procCnt.Load(),
		Errors: errCnt.Load(),
		Time:   time.Since(startTs).Truncate(time.Second),
	}, err
}
