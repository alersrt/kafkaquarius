package internal

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/consumer"
	"kafkaquarius/internal/domain"
	"kafkaquarius/internal/filter"
	"os"
	"sync/atomic"
	"time"
)

type App struct {
	cmd   string
	pCons *consumer.ParallelConsumer
	filt  *filter.Filter
	cfg   *config.Config
	stats struct {
		startTs  time.Time
		totalCnt atomic.Uint64
		foundCnt atomic.Uint64
		procCnt  atomic.Uint64
		errCnt   atomic.Uint64
	}
}

func NewApp(cmd string, cfg *config.Config) (*App, error) {
	consCfg := kafka.ConfigMap{
		"bootstrap.servers":    cfg.SourceBroker,
		"group.id":             cfg.ConsumerGroup,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"enable.partition.eof": true,
	}
	pCons, err := consumer.NewParallelConsumer(cfg.ThreadsNumber, cfg.SinceTime, cfg.ToTime, cfg.SourceTopic, consCfg)
	if err != nil {
		return nil, err
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return nil, err
	}
	filt, err := filter.NewFilter(string(filtCont))
	if err != nil {
		return nil, err
	}

	return &App{
		cmd:   cmd,
		pCons: pCons,
		filt:  filt,
		cfg:   cfg,
	}, nil
}

func (a *App) Close() {
	a.pCons.Close()
}

func (a *App) Stats() domain.Stats {
	return domain.Stats{
		Total:   a.stats.totalCnt.Load(),
		Found:   a.stats.foundCnt.Load(),
		Proc:    a.stats.procCnt.Load(),
		Errors:  a.stats.errCnt.Load(),
		Time:    time.Since(a.stats.startTs).Truncate(time.Millisecond),
		Threads: a.pCons.Threads(),
		Offsets: a.pCons.Offsets(),
	}
}

func (a *App) Execute(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	a.stats.startTs = time.Now()

	var err error
	switch a.cmd {
	case config.CmdMigrate:
		err = a.migrate(ctx)
	case config.CmdSearch:
		err = a.search(ctx)
	}

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, ctx.Err()) {
		err = errors.Join(err, cause)
	}

	return err
}

func (a *App) migrate(ctx context.Context) error {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": a.cfg.TargetBroker,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	return a.pCons.Do(
		ctx,
		func(err error) {
			a.stats.errCnt.Add(1)
		},
		func(msg *kafka.Message) {
			a.stats.totalCnt.Add(1)
		},
		func(msg *kafka.Message) {
			if ok, _ := a.filt.Eval(msg); ok {
				msg.TopicPartition = kafka.TopicPartition{Topic: &a.cfg.TargetTopic, Partition: kafka.PartitionAny}
				err := prod.Produce(msg, nil)
				if err != nil {
					a.stats.errCnt.Add(1)
				} else {
					a.stats.procCnt.Add(1)
				}
			}
		},
	)
}

func (a *App) search(ctx context.Context) error {
	var file *os.File
	var err error
	if a.cfg.OutputFile != "" {
		file, err = os.Create(a.cfg.OutputFile)
		if err != nil {
			return err
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

	return a.pCons.Do(
		ctx,
		func(err error) {
			a.stats.totalCnt.Add(1)
			a.stats.errCnt.Add(1)
		},
		func(msg *kafka.Message) {
			a.stats.totalCnt.Add(1)
			if ok, _ := a.filt.Eval(msg); ok {
				a.stats.foundCnt.Add(1)
				err := write(msg)
				if err != nil {
					a.stats.errCnt.Add(1)
				} else {
					a.stats.procCnt.Add(1)
				}
			}
		},
	)
}
