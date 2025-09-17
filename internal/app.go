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
	cel   *cel.Cel
	cfg   *config.Config
	stats struct {
		startTs  time.Time
		totalCnt atomic.Uint64
		foundCnt atomic.Uint64
		procCnt  atomic.Uint64
		errCnt   atomic.Uint64
	}
}

func (a *App) Init(cmd string, cfg *config.Config) error {
	switch cmd {
	case config.CmdMigrate, config.CmdSearch:
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
	}

	temp, err := os.ReadFile(cfg.TemplateFile)
	if err != nil {
		return err
	}
	a.cel, err = cel.NewCel(string(temp))
	if err != nil {
		return err
	}

	a.cfg = cfg
	return nil
}

func (a *App) Close() {
	if a.pCons != nil {
		a.pCons.Close()
	}
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
	case config.CmdProduce:
		err = a.produce(ctx)
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
			bytes, _ := json.Marshal(domain.FromKafkaWithAny(msg))
			boolVal, err := a.cel.Eval(bytes)
			if err != nil {
				a.stats.errCnt.Add(1)
			} else {
				if ok, val := boolVal.(bool); ok && val {
					msg.TopicPartition = kafka.TopicPartition{Topic: &a.cfg.TargetTopic, Partition: kafka.PartitionAny}
					err := prod.Produce(msg, nil)
					if err != nil {
						a.stats.errCnt.Add(1)
					} else {
						a.stats.procCnt.Add(1)
					}
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
		bytes, err := json.Marshal(domain.FromKafkaWithStrings(msg))
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
			bytes, _ := json.Marshal(domain.FromKafkaWithAny(msg))
			boolVal, err := a.cel.Eval(bytes)
			if err != nil {
				a.stats.errCnt.Add(1)
			} else {
				if ok, val := boolVal.(bool); ok && val {
					a.stats.foundCnt.Add(1)
					err := write(msg)
					if err != nil {
						a.stats.errCnt.Add(1)
					} else {
						a.stats.procCnt.Add(1)
					}
				}
			}
		},
	)
}

func (a *App) produce(ctx context.Context) error {
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
			ev, err := a.cel.Eval([]byte(scanner.Text()))
			if err != nil {
				return err
			}
			msg := &kafka.Message{}
			if err = json.Unmarshal(ev.([]byte), msg); err != nil {
				return err
			}
			if err := prod.Produce(msg, nil); err != nil {
				return err
			}
		}
	}
}
