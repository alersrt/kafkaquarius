package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	CmdMigrate = "migrate"
	CmdSearch  = "search"
)

type Config struct {
	FilterFile       string    `json:"filter_file,omitempty"`
	OutputFile       string    `json:"output_file,omitempty"`
	SourceBroker     string    `json:"source_broker,omitempty"`
	TargetBroker     string    `json:"target_broker,omitempty"`
	SourceTopic      string    `json:"source_topic,omitempty"`
	TargetTopic      string    `json:"target_topic,omitempty"`
	ConsumerGroup    string    `json:"consumer_group,omitempty"`
	PartitionsNumber int       `json:"partitions_number,omitempty"`
	SinceTime        time.Time `json:"since_time,omitempty"`
	ToTime           time.Time `json:"to_time"`
}

// NewConfig parses flags and returns list of parsed values in the Config struct.
func NewConfig(args []string) (string, *Config, error) {
	cfg := new(Config)

	sinceTime := int64(0)
	toTime := int64(0)

	migrateSet := flag.NewFlagSet(CmdMigrate, flag.ExitOnError)
	migrateSet.StringVar(&cfg.FilterFile, "filter-file", "", "required")
	migrateSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
	migrateSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
	migrateSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
	migrateSet.StringVar(&cfg.TargetBroker, "target-broker", "", "--source-broker is used if empty")
	migrateSet.StringVar(&cfg.TargetTopic, "target-topic", "", "--source-topic is used if empty")
	migrateSet.IntVar(&cfg.PartitionsNumber, "partitions-number", 1, "")
	migrateSet.Int64Var(&sinceTime, "since-time", 0, "unix epoch time")
	migrateSet.Int64Var(&toTime, "to-time", 0, "unix epoch time, now by default")

	searchSet := flag.NewFlagSet(CmdSearch, flag.ExitOnError)
	searchSet.StringVar(&cfg.FilterFile, "filter-file", "", "required")
	searchSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
	searchSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
	searchSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
	searchSet.StringVar(&cfg.OutputFile, "output-file", "", "")
	searchSet.IntVar(&cfg.PartitionsNumber, "partitions-number", 1, "")
	searchSet.Int64Var(&sinceTime, "since-time", 0, "unix epoch time")
	searchSet.Int64Var(&toTime, "to-time", 0, "unix epoch time, now by default")

	flag.Usage = func() {
		_, err := fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n%s\n%s\n", os.Args[0], CmdMigrate, CmdSearch)
		if err != nil {
			return
		}
		flag.CommandLine.PrintDefaults()
	}

	if len(args) < 2 {
		flag.Usage()
		return "", nil, nil
	}

	cmd := args[1]
	var valErrs error

	switch cmd {
	case CmdMigrate:
		if len(args) < 3 {
			migrateSet.Usage()
			return "", nil, nil
		}

		if err := migrateSet.Parse(args[2:]); err != nil {
			return CmdMigrate, nil, err
		}

		if cfg.FilterFile == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --filter-file"))
		}
		if cfg.ConsumerGroup == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --consumer-group"))
		}
		if cfg.SourceBroker == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --source-broker"))
		}
		if cfg.SourceTopic == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --source-topic"))
		}

		if cfg.TargetBroker == "" {
			cfg.TargetBroker = cfg.SourceBroker
		}
		if cfg.TargetTopic == "" {
			cfg.TargetTopic = cfg.SourceTopic
		}

	case CmdSearch:
		if len(args) < 3 {
			searchSet.Usage()
			return "", nil, nil
		}

		if err := searchSet.Parse(args[2:]); err != nil {
			return CmdSearch, nil, err
		}

		if cfg.FilterFile == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --filter-file"))
		}
		if cfg.ConsumerGroup == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --consumer-group"))
		}
		if cfg.SourceBroker == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --source-broker"))
		}
		if cfg.SourceTopic == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --source-topic"))
		}

	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}

	if sinceTime > toTime {
		errors.Join(valErrs, fmt.Errorf("cfg: --since-time must be before --to-time"))
	}

	if valErrs != nil {
		return cmd, nil, valErrs
	}

	cfg.SinceTime = time.Unix(sinceTime, 0)
	if toTime == 0 {
		cfg.ToTime = time.Now()
	} else {
		cfg.ToTime = time.Unix(toTime, 0)
	}

	return cmd, cfg, nil
}
