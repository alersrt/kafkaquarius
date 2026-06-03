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
	CmdProduce = "produce"
)

// List layouts from the most specific to the most generic
var iso8601Layouts = []string{
	time.RFC3339,           // 2006-01-02T15:04:05Z07:00
	time.RFC3339Nano,       // 2006-01-02T15:04:05.999999999Z07:00
	"2006-01-02T15:04:05",  // Missing timezone offset
	"20060102T150405Z0700", // Basic compressed layout
	"2006-01-02",           // Date only (ISO 8601 / time.DateOnly)
}

func ParseFlexibleISO8601(val string) (time.Time, error) {
	for _, layout := range iso8601Layouts {
		if t, err := time.Parse(layout, val); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("failed to parse string with any known ISO8601 layout")
}

type Config struct {
	FilterFile    string        `json:"filter_file,omitempty"`
	TemplateFile  string        `json:"template_file,omitempty"`
	OutputFile    string        `json:"output_file,omitempty"`
	SourceFile    string        `json:"source_file,omitempty"`
	SourceBroker  string        `json:"source_broker,omitempty"`
	TargetBroker  string        `json:"target_broker,omitempty"`
	SourceTopic   string        `json:"source_topic,omitempty"`
	TargetTopic   string        `json:"target_topic,omitempty"`
	ConsumerGroup string        `json:"consumer_group,omitempty"`
	ThreadsNumber int           `json:"threads_number,omitempty"`
	SinceTime     time.Time     `json:"since_time,omitempty"`
	ToTime        time.Time     `json:"to_time,omitempty"`
	FlushTimeout  time.Duration `json:"flush_timeout,omitempty"`
}

// NewConfig parses flags and returns list of parsed values in the Config struct.
func NewConfig(args []string) (string, *Config, error) {
	cfg := new(Config)

	sinceTime := ""
	toTime := ""
	flushTimeout := ""
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
	migrateSet.StringVar(&sinceTime, "since-time", "1970-01-01T00:00:00", "ISO-8601 datetime")
	migrateSet.StringVar(&toTime, "to-time", "9999-12-31T23:59:59", "ISO-8601 datetime")
	migrateSet.BoolVar(&leeroy, "leeroy", false, "fatuity and courage")

	searchSet := flag.NewFlagSet(CmdSearch, flag.ExitOnError)
	searchSet.StringVar(&cfg.FilterFile, "filter-file", "", "required, CEL filter")
	searchSet.StringVar(&cfg.TemplateFile, "template-file", "", "optional, CEL transform")
	searchSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
	searchSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
	searchSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
	searchSet.StringVar(&cfg.OutputFile, "output-file", "", "")
	searchSet.IntVar(&cfg.ThreadsNumber, "threads-number", 1, "")
	searchSet.StringVar(&sinceTime, "since-time", "1970-01-01T00:00:00", "ISO-8601 datetime")
	searchSet.StringVar(&toTime, "to-time", "9999-12-31T23:59:59", "ISO-8601 datetime")

	produceSet := flag.NewFlagSet(CmdProduce, flag.ExitOnError)
	produceSet.StringVar(&cfg.TargetBroker, "target-broker", "", "required")
	produceSet.StringVar(&cfg.TargetTopic, "target-topic", "", "required")
	produceSet.StringVar(&cfg.SourceFile, "source-file", "", "required, JSONL")
	produceSet.StringVar(&cfg.TemplateFile, "template-file", "", "required, CEL transform")
	produceSet.StringVar(&cfg.FilterFile, "filter-file", "", "optional, CEL filter")
	produceSet.StringVar(&flushTimeout, "flush-timeout", "5m", "optional, set the producer flush timeout")

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
		var err error
		cfg.FlushTimeout, err = time.ParseDuration(flushTimeout)
		if err != nil {
			valErrs = errors.Join(valErrs, fmt.Errorf("cfg: --flush-timeout has wrong format"))
		}

	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}

	var err error
	cfg.SinceTime, err = ParseFlexibleISO8601(sinceTime)
	if err != nil {
		valErrs = errors.Join(valErrs, fmt.Errorf("cfg: --since-time has wrong format"))
	}
	cfg.ToTime, err = ParseFlexibleISO8601(toTime)
	if err != nil {
		valErrs = errors.Join(valErrs, fmt.Errorf("cfg: --to-time has wrong format"))
	}

	if cfg.SinceTime.After(cfg.ToTime) {
		valErrs = errors.Join(valErrs, fmt.Errorf("cfg: --since-time must be before --to-time"))
	}

	if valErrs != nil {
		return cmd, nil, valErrs
	}

	return cmd, cfg, nil
}
