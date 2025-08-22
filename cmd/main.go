package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/cel-go/cel"
	"log/slog"
	"os"
	"time"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Key       any       `json:"key"`
	Value     any       `json:"value"`
	Headers   []any     `json:"headers"`
}

func main() {
	cfg := config()

	filter, err := os.ReadFile(cfg.FilterPath)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	env, err := cel.NewEnv(
		cel.Variable("key", cel.AnyType),
		cel.Variable("value", cel.AnyType),
		cel.Variable("headers", cel.ListType(cel.AnyType)),
		cel.Variable("timestamp", cel.TimestampType),
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	ast, iss := env.Compile(string(filter))
	if iss != nil && iss.Err() != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	prog, err := env.Program(ast)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	var inInterface map[string]any

	inrec, _ := json.Marshal(&Message{Key: "test_key", Value: map[string]any{"some": 0}})
	_ = json.Unmarshal(inrec, &inInterface)

	eval, _, err := prog.Eval(inInterface)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	fmt.Printf("%+v", eval)
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
