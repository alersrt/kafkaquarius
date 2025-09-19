package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"kafkaquarius/internal/cel"
	"kafkaquarius/internal/config"
	"kafkaquarius/internal/consumer"
	"kafkaquarius/internal/domain"
	"log/slog"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type App struct {
	pCons     *consumer.ParallelConsumer
	transform *cel.Cel
	filter    *cel.Cel
	cfg       *config.Config
	stats     struct {
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

	if cfg.FilterFile != "" {
		filter, err := os.ReadFile(cfg.FilterFile)
		if err != nil {
			return err
		}
		a.filter, err = cel.NewCel(string(filter))
		if err != nil {
			return err
		}
	}

	if cfg.TemplateFile != "" {
		temp, err := os.ReadFile(cfg.TemplateFile)
		if err != nil {
			return err
		}
		a.transform, err = cel.NewCel(string(temp))
		if err != nil {
			return err
		}
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

			found, err := a.check(msg, false)
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("check: %v: %+v", err, msg))
				return
			}
			if !found {
				return
			}
			a.stats.foundCnt.Add(1)

			dst, err := a.eval(msg, reflect.TypeFor[*kafka.Message]())
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("eval: %v: %+v", err, msg))
				return
			}

			kMsg := dst.(*kafka.Message)
			kMsg.TopicPartition = kafka.TopicPartition{Topic: &a.cfg.TargetTopic, Partition: kafka.PartitionAny}
			if err := prod.Produce(kMsg, nil); err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("produce: %v: %+v", err, kMsg))
				return
			}
			a.stats.procCnt.Add(1)
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

	write := func(msg any) error {
		if file == nil {
			return nil
		}
		bytes, err := json.Marshal(msg)
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

			found, err := a.check(msg, false)
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("check: %v: %+v", err, msg))
				return
			}
			if !found {
				return
			}
			a.stats.foundCnt.Add(1)

			dst, err := a.eval(msg, reflect.TypeFor[map[string]any]())
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("eval: %v: %+v", err, msg))
				return
			}

			err = write(dst)
			if err != nil {
				slog.Error(fmt.Sprintf("write: %v: %+v", err, dst))
				a.stats.errCnt.Add(1)
			} else {
				a.stats.procCnt.Add(1)
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
	timeoutMs := 5 * 1000
	for {
		select {
		case <-ctx.Done():
			prod.Flush(timeoutMs)
			return nil
		default:
			ok := scanner.Scan()
			if !ok {
				prod.Flush(timeoutMs)
				return scanner.Err()
			}
			a.stats.totalCnt.Add(1)

			obj := domain.Des([]byte(scanner.Text()))
			found, err := a.check(scanner.Text(), true)
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("check: %v: %+v", err, obj))
				return err
			}
			if !found {
				continue
			}
			a.stats.foundCnt.Add(1)

			dst, err := a.eval(obj, reflect.TypeFor[*kafka.Message]())
			if err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("eval: %v: %+v", err, obj))
				return err
			}

			kMsg := dst.(*kafka.Message)
			kMsg.TopicPartition = kafka.TopicPartition{Topic: &a.cfg.TargetTopic, Partition: kafka.PartitionAny}
			if err := prod.Produce(kMsg, nil); err != nil {
				a.stats.errCnt.Add(1)
				slog.Error(fmt.Sprintf("produce: %v: %+v", err, kMsg))
				return err
			}
			a.stats.procCnt.Add(1)
		}
	}
}

func (a *App) check(self any, def bool) (bool, error) {
	if a.filter != nil {
		ev, err := a.filter.Eval(self, reflect.TypeFor[bool]())
		if err != nil {
			return false, err
		}
		def = ev.(bool)
	}
	return def, nil
}

func (a *App) eval(self any, typeDesc reflect.Type) (any, error) {
	if a.transform != nil {
		return a.transform.Eval(self, typeDesc)
	}
	return self, nil
}
