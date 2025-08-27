package internal

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/domain"
	"kafkaquarius/internal/filter"
	"os"
	"sync/atomic"
	"time"
)

func Migrate(ctx context.Context, cfg *config.Config) (stats *domain.Stats, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	startTs := time.Now()
	var totalCnt atomic.Uint64
	var foundCnt atomic.Uint64
	var procCnt atomic.Uint64
	var errCnt atomic.Uint64
	interOp := make(chan *kafka.Message)
	defer close(interOp)

	for i := 0; i < cfg.PartitionsNumber; i++ {
		go func() {
			err = consume(ctx, cfg, i, interOp, startTs, &totalCnt, &foundCnt, &errCnt)
			cancel()
		}()
	}

	go func() {
		var prod *kafka.Producer
		prod, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": cfg.TargetBroker,
		})
		if err != nil {
			return
		}
		defer prod.Close()

		for msg := range interOp {
			msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
			err = prod.Produce(msg, nil)
			if err != nil {
				errCnt.Add(1)
				continue
			} else {
				procCnt.Add(1)
			}
		}
	}()

	<-ctx.Done()

	return &domain.Stats{
		Total:  totalCnt.Load(),
		Found:  foundCnt.Load(),
		Proc:   procCnt.Load(),
		Errors: errCnt.Load(),
		Time:   time.Since(startTs),
	}, nil
}

func Search(ctx context.Context, cfg *config.Config) (stats *domain.Stats, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	startTs := time.Now()
	var totalCnt atomic.Uint64
	var foundCnt atomic.Uint64
	var procCnt atomic.Uint64
	var errCnt atomic.Uint64
	interOp := make(chan *kafka.Message)
	defer close(interOp)

	for i := 0; i < cfg.PartitionsNumber; i++ {
		go func() {
			err = consume(ctx, cfg, i, interOp, startTs, &totalCnt, &foundCnt, &errCnt)
			cancel()
		}()
	}

	go func(interOp chan *kafka.Message, procCnt *atomic.Uint64) {
		var file *os.File
		file, err = os.Create(cfg.OutputFile)
		if err != nil {
			cancel()
		}
		defer func(file *os.File) {
			_ = file.Close()
		}(file)

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-interOp:
				bytesMsg, _ := json.Marshal(domain.FromKafka(msg))
				_, _ = file.Write(bytesMsg)
				_, _ = file.WriteString("\n")
				procCnt.Add(1)
			}
		}
	}(interOp, &procCnt)

	<-ctx.Done()

	return &domain.Stats{
		Total:  totalCnt.Load(),
		Found:  foundCnt.Load(),
		Proc:   procCnt.Load(),
		Errors: errCnt.Load(),
		Time:   time.Since(startTs),
	}, nil
}

func consume(ctx context.Context, cfg *config.Config, i int, interOp chan *kafka.Message,
	startTs time.Time, totalCnt *atomic.Uint64, foundCnt *atomic.Uint64, errCnt *atomic.Uint64) error {
	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return err
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return err
	}

	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.SourceBroker,
		"group.id":           cfg.ConsumerGroup,
		"client.id":          i,
		"auto.offset.reset":  kafka.OffsetBeginning.String(),
		"enable.auto.commit": false,
	})
	if err != nil {
		return err
	}
	defer func(cons *kafka.Consumer) {
		_ = cons.Close()
	}(cons)

	err = cons.Subscribe(cfg.SourceTopic, nil)
	if err != nil {
		return err
	}
	defer func(cons *kafka.Consumer) {
		_ = cons.Unsubscribe()
	}(cons)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := cons.ReadMessage(time.Minute)
			if err != nil {
				if err != nil && err.(kafka.Error).IsTimeout() {
					return nil
				}
				errCnt.Add(1)
				continue
			}
			if startTs.Before(msg.Timestamp) {
				return nil
			}

			totalCnt.Add(1)

			ok, err := filt.Eval(msg)
			if err != nil {
				continue
			}

			if ok {
				foundCnt.Add(1)
				interOp <- msg
			}
		}
	}
}
