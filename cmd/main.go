package main

import (
	"flag"
	"fmt"
	"kafkaquarius/internal"
	"log/slog"
	"os"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

func main() {
	cfg := config()

	filterContent, err := os.ReadFile(cfg.FilterPath)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	_, err = internal.NewFilter(string(filterContent))
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	os.Exit(ExitCodeDone)
}

type Config struct {
	SourceBroker  string `json:"source_broker"`
	TargetBroker  string `json:"target_broker"`
	SourceTopic   string `json:"source_topic"`
	TargetTopic   string `json:"target_topic"`
	ConsumerGroup string `json:"consumer_group"`
	FilterPath    string `json:"filter_path"`
}

// config parses flags and returns list of parsed values in the Config struct.
func config() *Config {
	cfg := new(Config)

	flag.StringVar(&cfg.SourceBroker, "source-broker", "", "")
	flag.StringVar(&cfg.TargetBroker, "target-broker", "", "")
	flag.StringVar(&cfg.SourceTopic, "source-topic", "", "")
	flag.StringVar(&cfg.TargetTopic, "target-topic", "", "")
	flag.StringVar(&cfg.ConsumerGroup, "consumer-group", "", "")
	flag.StringVar(&cfg.FilterPath, "filter-path", "", "")

	flag.Parse()
	return cfg
}
