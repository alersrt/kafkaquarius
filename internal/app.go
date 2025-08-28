package internal

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/domain"
	"kafkaquarius/internal/filter"
	"os"
	"sync/atomic"
	"time"
)

func Execute(ctx context.Context, cmd string, cfg *config.Config) (*domain.Stats, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	startTs := time.Now()
	var totalCnt atomic.Uint64
	var foundCnt atomic.Uint64
	var procCnt atomic.Uint64
	var errCnt atomic.Uint64

	interOp := make(chan *kafka.Message)
	defer close(interOp)

	for i := 0; i < cfg.ThreadsNumber; i++ {
		go func() {
			err := consume(ctx, cfg, i, interOp, &totalCnt, &foundCnt, &errCnt)
			cancel(err)
		}()
	}

	var err error
	switch cmd {
	case config.CmdMigrate:
		err = migrate(ctx, cfg, interOp, &procCnt, &errCnt)
	case config.CmdSearch:
		err = search(ctx, cfg, interOp, &procCnt, &errCnt)
	}

	<-ctx.Done()

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

func migrate(ctx context.Context, cfg *config.Config, interOp chan *kafka.Message,
	procCnt *atomic.Uint64, errCnt *atomic.Uint64) error {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.TargetBroker,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-interOp:
			msg.TopicPartition = kafka.TopicPartition{Topic: &cfg.TargetTopic, Partition: kafka.PartitionAny}
			err := prod.Produce(msg, nil)
			if err != nil {
				errCnt.Add(1)
				continue
			} else {
				procCnt.Add(1)
			}
		}
	}
}

func search(ctx context.Context, cfg *config.Config, interOp chan *kafka.Message,
	procCnt *atomic.Uint64, errCnt *atomic.Uint64) error {
	file, err := os.Create(cfg.OutputFile)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	write := func(msg *kafka.Message) error {
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

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-interOp:
			err := write(msg)
			if err != nil {
				errCnt.Add(1)
			} else {
				procCnt.Add(1)
			}
		}
	}
}

func consume(ctx context.Context, cfg *config.Config, i int, interOp chan *kafka.Message,
	totalCnt *atomic.Uint64, foundCnt *atomic.Uint64, errCnt *atomic.Uint64) error {
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

	timeoutMs := 5 * time.Second.Milliseconds()

	md, err := cons.GetMetadata(&cfg.SourceTopic, false, int(timeoutMs))
	if err != nil {
		return err
	}

	part := calcPart(i, len(md.Topics[cfg.SourceTopic].Partitions), cfg.ThreadsNumber)
	if part == nil {
		return nil
	}

	parts := make([]kafka.TopicPartition, len(part))
	for _, p := range part {
		parts = append(parts, kafka.TopicPartition{
			Topic:     &cfg.SourceTopic,
			Partition: int32(p),
			Offset:    kafka.Offset(cfg.SinceTime.UnixMilli()),
		})
	}

	parts, err = cons.OffsetsForTimes(parts, int(timeoutMs))
	if err != nil {
		return err
	}
	err = cons.Assign(parts)
	if err != nil {
		return err
	}

	defer func(cons *kafka.Consumer) {
		_ = cons.Unassign()
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
			if cfg.ToTime.Before(msg.Timestamp) {
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

// calcPart returns list with partition numbers. Numeration is started from zero.
func calcPart(i int, parts int, threads int) []int {
	if i > parts {
		return nil
	}
	div := parts / threads
	rem := parts % threads

	if div == 0 {
		if i < parts {
			return []int{i}
		} else {
			return nil
		}
	} else {
		if i*threads <= parts {
			res := make([]int, threads)
			for j := 0; j < threads; j++ {
				res[j] = i*threads + j
			}
			return res
		} else {
			res := make([]int, rem)
			for j := 0; j < rem; j++ {
				res[j] = i*rem + j
			}
			return res
		}
	}
}
