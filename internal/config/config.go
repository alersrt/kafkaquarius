package config

import (
	"errors"
	"flag"
	"fmt"
)

const (
	CmdMigrate = "migrate"
	CmdSearch  = "search"
	CmdStats   = "stats"
)

type Config struct {
	Cmd           string `json:"cmd"`
	FilterFile    string `json:"filter_file,omitempty"`
	OutputFile    string `json:"output_file,omitempty"`
	SourceBroker  string `json:"source_broker,omitempty"`
	TargetBroker  string `json:"target_broker,omitempty"`
	SourceTopic   string `json:"source_topic,omitempty"`
	TargetTopic   string `json:"target_topic,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
}

// NewConfig parses flags and returns list of parsed values in the Config struct.
func NewConfig(args []string) (string, *Config, error) {
	cfg := new(Config)
	cfg.Cmd = args[1]
	switch cfg.Cmd {
	case CmdMigrate:
		migrateSet := flag.NewFlagSet(CmdMigrate, flag.ExitOnError)
		migrateSet.StringVar(&cfg.FilterFile, "filter-file", "", "required")
		migrateSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
		migrateSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
		migrateSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
		migrateSet.StringVar(&cfg.TargetBroker, "target-broker", "", "--source-broker is used if empty")
		migrateSet.StringVar(&cfg.TargetTopic, "target-topic", "", "--source-topic is used if empty")
		if err := migrateSet.Parse(args[2:]); err != nil {
			return CmdMigrate, nil, err
		}

		var valErrs error
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
		if valErrs != nil {
			return CmdMigrate, nil, valErrs
		}

		if cfg.TargetBroker == "" {
			cfg.TargetBroker = cfg.SourceBroker
		}
		if cfg.TargetTopic == "" {
			cfg.TargetTopic = cfg.SourceTopic
		}

		return CmdMigrate, cfg, nil
	case CmdSearch:
		searchSet := flag.NewFlagSet(CmdSearch, flag.ExitOnError)
		searchSet.StringVar(&cfg.FilterFile, "filter-file", "", "required")
		searchSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
		searchSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
		searchSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
		searchSet.StringVar(&cfg.OutputFile, "output-file", "", "")
		if err := searchSet.Parse(args[2:]); err != nil {
			return CmdSearch, nil, err
		}

		var valErrs error
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
		if valErrs != nil {
			return CmdSearch, nil, valErrs
		}

		return CmdSearch, cfg, nil
	case CmdStats:
		statsSet := flag.NewFlagSet(CmdStats, flag.ExitOnError)
		statsSet.StringVar(&cfg.FilterFile, "filter-file", "", "required")
		statsSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "required")
		statsSet.StringVar(&cfg.SourceBroker, "source-broker", "", "required")
		statsSet.StringVar(&cfg.SourceTopic, "source-topic", "", "required")
		statsSet.StringVar(&cfg.OutputFile, "output-file", "", "")
		if err := statsSet.Parse(args[2:]); err != nil {
			return CmdStats, nil, err
		}

		var valErrs error
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
		if valErrs != nil {
			return CmdStats, nil, valErrs
		}

		return CmdStats, cfg, nil
	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}
}
