package main

import (
	"flag"
	"fmt"
	"kafkaquarius/internal/filter"
	"log/slog"
	"os"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

func main() {
	//ctx := context.Background()

	_, cfg, err := NewConfig()
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	filterContent, err := os.ReadFile(cfg.FilterPath)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	_, err = filter.NewFilter(string(filterContent))
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	os.Exit(ExitCodeDone)
}

const (
	CmdMigrate = "migrate"
	CmdSearch  = "search"
	CmdStats   = "stats"
)

type Config struct {
	FilterPath    string `json:"filter_path"`
	SourceBroker  string `json:"source_broker"`
	TargetBroker  string `json:"target_broker"`
	SourceTopic   string `json:"source_topic"`
	TargetTopic   string `json:"target_topic"`
	ConsumerGroup string `json:"consumer_group"`
}

// NewConfig parses flags and returns list of parsed values in the Config struct.
func NewConfig() (string, *Config, error) {
	cfg := new(Config)

	args := os.Args
	switch args[1] {
	case CmdMigrate:
		migrateSet := flag.NewFlagSet(CmdMigrate, flag.ExitOnError)
		migrateSet.StringVar(&cfg.FilterPath, "filter-path", "", "")
		migrateSet.StringVar(&cfg.SourceBroker, "source-broker", "", "")
		migrateSet.StringVar(&cfg.TargetBroker, "target-broker", "", "")
		migrateSet.StringVar(&cfg.SourceTopic, "source-topic", "", "")
		migrateSet.StringVar(&cfg.TargetTopic, "target-topic", "", "")
		migrateSet.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "")
		if err := migrateSet.Parse(args[2:]); err != nil {
			return CmdMigrate, nil, err
		}
		return CmdMigrate, cfg, nil
	case CmdSearch:
		return CmdSearch, nil, fmt.Errorf("not implemented")
	case CmdStats:
		return CmdStats, nil, fmt.Errorf("not implemented")
	default:
		return "", nil, fmt.Errorf("wrong cmd")
	}
}
