package config

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"time"
)

const (
	CmdMigrate = "migrate"
	CmdSearch  = "search"
	CmdProduce = "produce"
)

const (
	MaxSec = math.MaxInt64 - (1969*365+1969/4-1969/100+1969/400)*24*60*60
)

type Config struct {
	FilterFile    string    `json:"filter_file,omitempty"`
	TemplateFile  string    `json:"template_file,omitempty"`
	OutputFile    string    `json:"output_file,omitempty"`
	SourceFile    string    `json:"source_file,omitempty"`
	SourceBroker  string    `json:"source_broker,omitempty"`
	TargetBroker  string    `json:"target_broker,omitempty"`
	SourceTopic   string    `json:"source_topic,omitempty"`
	TargetTopic   string    `json:"target_topic,omitempty"`
	ConsumerGroup string    `json:"consumer_group,omitempty"`
	ThreadsNumber int       `json:"threads_number,omitempty"`
	SinceTime     time.Time `json:"since_time,omitempty"`
	ToTime        time.Time `json:"to_time,omitempty"`
}

// NewConfig parses flags and returns list of parsed values in the Config struct.
func NewConfig(args []string) (string, *Config, error) {
	cfg := new(Config)

	sinceTime := int64(0)
	toTime := int64(0)
	leeroy := false

	migrateSet := flag.NewFlagSet(CmdMigrate, flag.ExitOnError)
	migrateSet.StringVar(&cfg.FilterFile, "filter-file", "", "required, CEL filter")
	migrateSet.StringVar(&cfg.TemplateFile, "template-file", "", "optional, CEL transform")
	migrateSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
	migrateSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
	migrateSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
	migrateSet.StringVar(&cfg.TargetBroker, "target-broker", "", "--source-broker is used if empty")
	migrateSet.StringVar(&cfg.TargetTopic, "target-topic", "", "--source-topic is used if empty")
	migrateSet.IntVar(&cfg.ThreadsNumber, "threads-number", 1, "")
	migrateSet.Int64Var(&sinceTime, "since-time", 0, "unix epoch time, 0 by default")
	migrateSet.Int64Var(&toTime, "to-time", 0, "unix epoch time, infinity by default")
	migrateSet.BoolVar(&leeroy, "leeroy", false, "fatuity and courage")

	searchSet := flag.NewFlagSet(CmdSearch, flag.ExitOnError)
	searchSet.StringVar(&cfg.FilterFile, "filter-file", "", "required, CEL filter")
	searchSet.StringVar(&cfg.TemplateFile, "template-file", "", "optional, CEL transform")
	searchSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
	searchSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
	searchSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
	searchSet.StringVar(&cfg.OutputFile, "output-file", "", "")
	searchSet.IntVar(&cfg.ThreadsNumber, "threads-number", 1, "")
	searchSet.Int64Var(&sinceTime, "since-time", 0, "unix epoch time, 0 by default")
	searchSet.Int64Var(&toTime, "to-time", 0, "unix epoch time, infinity by default")

	produceSet := flag.NewFlagSet(CmdProduce, flag.ExitOnError)
	produceSet.StringVar(&cfg.TargetBroker, "target-broker", "", "required")
	produceSet.StringVar(&cfg.TargetTopic, "target-topic", "", "required")
	produceSet.StringVar(&cfg.SourceFile, "source-file", "", "required, JSONL")
	produceSet.StringVar(&cfg.TemplateFile, "template-file", "", "required, CEL transform")
	produceSet.StringVar(&cfg.FilterFile, "filter-file", "", "optional, CEL filter")

	flag.Usage = func() {
		_, err := fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n%s\n%s\n%s\n", os.Args[0], CmdMigrate, CmdSearch, CmdProduce)
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

		if cfg.SourceBroker == cfg.TargetBroker && cfg.SourceTopic == cfg.TargetTopic && !leeroy {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: not Leeroy: the source coincides with the destination"))
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

	case CmdProduce:
		if len(args) < 3 {
			produceSet.Usage()
			return "", nil, nil
		}

		if err := produceSet.Parse(args[2:]); err != nil {
			return CmdProduce, nil, err
		}

		if cfg.TargetBroker == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --target-broker"))
		}
		if cfg.TargetTopic == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --target-topic"))
		}
		if cfg.SourceFile == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --source-file"))
		}
		if cfg.TemplateFile == "" {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: missed --template-file"))
		}

	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}

	if sinceTime == 0 {
		cfg.SinceTime = time.Time{}
	} else {
		cfg.SinceTime = time.Unix(sinceTime, 0)
	}
	if toTime == 0 {
		cfg.ToTime = time.Unix(MaxSec, 0)
	} else {
		cfg.ToTime = time.Unix(toTime, 0)
	}

	if cfg.SinceTime.After(cfg.ToTime) {
		valErrs = errors.Join(valErrs, fmt.Errorf("cfg: --since-time must be before --to-time"))
	}

	if valErrs != nil {
		return cmd, nil, valErrs
	}

	return cmd, cfg, nil
}
