package main

import (
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/cel-go/cel"
	"log/slog"
	"os"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

func main() {
	cfg := config()

	file, err := os.ReadFile(cfg.FilterPath)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
	}

	env, err := cel.NewEnv(
		cel.Types(&kafka.Message{}),
		cel.Variable("record", cel.ObjectType("kafka.Message")),
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}
	fmt.Printf("%+v", env)

	fmt.Printf("%+v\n", file)
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

// config parses os.Args and returns list of parsed values in the Config struct.
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
