package config

import (
	"flag"
	"fmt"
)

const (
	CmdMigrate = "migrate"
	CmdSearch  = "search"
	CmdStats   = "stats"
)

type Config struct {
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

	switch args[1] {
	case CmdMigrate:
		migrateSet := flag.NewFlagSet(CmdMigrate, flag.ExitOnError)
		migrateSet.StringVar(&cfg.FilterFile, "filter-file", "filter.txt", "")
		migrateSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "kafkaquarius", "")
		migrateSet.StringVar(&cfg.SourceBroker, "source-broker", "", "")
		migrateSet.StringVar(&cfg.TargetBroker, "target-broker", "", "")
		migrateSet.StringVar(&cfg.SourceTopic, "source-topic", "", "")
		migrateSet.StringVar(&cfg.TargetTopic, "target-topic", "", "")
		if err := migrateSet.Parse(args[2:]); err != nil {
			return CmdMigrate, nil, err
		}
		return CmdMigrate, cfg, nil
	case CmdSearch:
		searchSet := flag.NewFlagSet(CmdSearch, flag.ExitOnError)
		searchSet.StringVar(&cfg.FilterFile, "filter-file", "filter.txt", "")
		searchSet.StringVar(&cfg.OutputFile, "output-file", "output.txt", "")
		searchSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "kafkaquarius", "")
		searchSet.StringVar(&cfg.SourceBroker, "source-broker", "", "")
		searchSet.StringVar(&cfg.SourceTopic, "source-topic", "", "")
		if err := searchSet.Parse(args[2:]); err != nil {
			return CmdSearch, nil, err
		}
		return CmdSearch, cfg, nil
	case CmdStats:
		statsSet := flag.NewFlagSet(CmdStats, flag.ExitOnError)
		statsSet.StringVar(&cfg.FilterFile, "filter-file", "filter.txt", "")
		statsSet.StringVar(&cfg.OutputFile, "output-file", "output.txt", "")
		statsSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "kafkaquarius", "")
		statsSet.StringVar(&cfg.SourceBroker, "source-broker", "", "")
		statsSet.StringVar(&cfg.SourceTopic, "source-topic", "", "")
		if err := statsSet.Parse(args[2:]); err != nil {
			return CmdStats, nil, err
		}
		return CmdStats, cfg, nil
	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}
}
