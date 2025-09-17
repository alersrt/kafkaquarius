package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"kafkaquarius/internal/cel"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/consumer"
	"kafkaquarius/internal/domain"
	"os"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type App struct {
	pCons *consumer.ParallelConsumer
	filt  *cel.Cel
	cfg   *config.Config
	stats struct {
		startTs  time.Time
		totalCnt atomic.Uint64
		foundCnt atomic.Uint64
		procCnt  atomic.Uint64
		errCnt   atomic.Uint64
	}
}

func (a *App) Init(cfg *config.Config) error {
	consCfg := kafka.ConfigMap{
		"bootstrap.servers":    cfg.SourceBroker,
		"group.id":             cfg.ConsumerGroup,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"enable.partition.eof": true,
	}
	var err error
	a.pCons, err = consumer.NewParallelConsumer(cfg.ThreadsNumber, cfg.SinceTime, cfg.ToTime, cfg.SourceTopic, consCfg)
	if err != nil {
		return err
	}

	filtCont, err := os.ReadFile(cfg.FilterFile)
	if err != nil {
		return err
	}
	a.filt, err = cel.NewCel(string(filtCont))
	if err != nil {
		return err
	}

	a.cfg = cfg
	return nil
}

func (a *App) Close() {
	a.pCons.Close()
}

func (a *App) Stats() domain.Stats {
	stats := new(domain.Stats)
	if a.pCons == nil {
		stats.Threads = -1
		stats.Offsets = nil
	} else {
		stats.Threads = a.pCons.Threads()
		stats.Offsets = a.pCons.Offsets()
	}

	stats.Total = a.stats.totalCnt.Load()
	stats.Found = a.stats.foundCnt.Load()
	stats.Proc = a.stats.procCnt.Load()
	stats.Errors = a.stats.errCnt.Load()

	if a.stats.startTs.IsZero() {
		stats.Time = time.Duration(0)
	} else {
		stats.Time = time.Since(a.stats.startTs).Truncate(time.Millisecond)
	}

	return *stats
}

func (a *App) Execute(ctx context.Context, cmd string) error {
	a.stats.startTs = time.Now()

	var err error
	switch cmd {
	case config.CmdMigrate:
		err = a.migrate(ctx)
	case config.CmdSearch:
		err = a.search(ctx)
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
			a.stats.totalCnt.Add(1)
			a.stats.errCnt.Add(1)
		},
		func(msg *kafka.Message) {
			a.stats.totalCnt.Add(1)
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

func (a *App) produce(ctx context.Context) error {
	tmp, err := os.ReadFile(a.cfg.TemplateFile)
	if err != nil {
		return err
	}

	source, err := os.Open(a.cfg.SourceFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = source.Close()
	}()

	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": a.cfg.TargetBroker,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	scanner := bufio.NewScanner(source)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			scanner.Scan()

			if err := prod.Produce(&kafka.Message{
				Value: []byte(scanner.Text()),
			}, nil); err != nil {
				return err
			}
		}
	}
}
