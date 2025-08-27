package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/domain"
	"kafkaquarius/internal/filter"
	"log/slog"
	"os"
	"time"
)

func Migrate(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var prod *kafka.Producer
	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.TargetBroker,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return err
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return err
	}

	startTs := time.Now()
	totalCnt := 0
	foundCnt := 0
	sentCnt := 0
	errCnt := 0
	foundChan := make(chan *kafka.Message)
	defer close(foundChan)

	for i := 0; i < cfg.PartitionsNumber; i++ {
		go func() {
			cons, err := consCreateAndSubscribe(cfg, i)
			if err != nil {
				return
			}
			defer cons.Close()

			for {
				msg, err := cons.ReadMessage(time.Minute)
				if err != nil {
					if err != nil && err.(kafka.Error).IsTimeout() {
						cancel()
						return
					}
					errCnt++
					continue
				}
				if startTs.Before(msg.Timestamp) {
					cancel()
					return
				}

				totalCnt++

				ok, err := filt.Eval(msg)
				if err != nil {
					continue
				}

				if ok {
					foundCnt++
					foundChan <- msg
				}
			}
		}()
	}

	go func() {
		for msg := range foundChan {
			msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
			err := prod.Produce(msg, nil)
			if err != nil {
				errCnt++
				continue
			} else {
				sentCnt++
			}
		}
	}()

	defer func() {
		slog.Info(fmt.Sprintf(`
total: %d
found: %d
sent: %d
errors: %d
time: %v
`, totalCnt, foundCnt, sentCnt, errCnt, time.Since(startTs)))
	}()

	<-ctx.Done()
	return nil
}

func Search(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return err
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return err
	}

	startTs := time.Now()
	totalCnt := 0
	foundCnt := 0
	writtenCnt := 0
	errCnt := 0
	var foundMsgs []*domain.Message
	foundChan := make(chan *domain.Message)
	defer close(foundChan)

	var file *os.File
	if cfg.OutputFile != "" {
		file, err = os.Create(cfg.OutputFile)
		if err != nil {
			return err
		}
		defer file.Close()
	}

	msgChan := make(chan *kafka.Message)
	defer close(msgChan)

	for i := 0; i < cfg.PartitionsNumber; i++ {
		go func() {
			cons, err := consCreateAndSubscribe(cfg, i)
			if err != nil {
				return
			}
			defer cons.Close()

			for {
				msg, err := cons.ReadMessage(time.Minute)
				if err != nil {
					if err != nil && err.(kafka.Error).IsTimeout() {
						cancel()
						return
					}
					errCnt++
					continue
				}
				if startTs.Before(msg.Timestamp) {
					cancel()
					return
				}

				totalCnt++

				ok, err := filt.Eval(msg)
				if err != nil {
					continue
				}

				if ok {
					foundCnt++
					if file != nil {
						foundChan <- domain.FromKafka(msg)
					}
				}
			}
		}()
	}

	go func() {
		for msg := range foundChan {
			foundMsgs = append(foundMsgs, msg)
			writtenCnt++
		}
	}()

	defer func() {
		if file != nil {
			bytesMsgs, _ := json.MarshalIndent(foundMsgs, "", "  ")
			_, _ = file.Write(bytesMsgs)
		}
		slog.Info(fmt.Sprintf(`stat:
total: %d
found: %d
written: %d
errors: %d
time: %v
`, totalCnt, foundCnt, writtenCnt, errCnt, time.Since(startTs)))
	}()

	<-ctx.Done()
	return nil
}

func consCreateAndSubscribe(cfg *config.Config, i int) (*kafka.Consumer, error) {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.SourceBroker,
		"group.id":           cfg.ConsumerGroup,
		"client.id":          i,
		"auto.offset.reset":  kafka.OffsetBeginning.String(),
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	err = cons.Subscribe(cfg.SourceTopic, nil)
	if err != nil {
		return nil, err
	}

	return cons, nil
}
