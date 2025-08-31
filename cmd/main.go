package main

import (
	"context"
	"fmt"
	"kafkaquarius/internal"
	"kafkaquarius/internal/config"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	ExitCodeDone         = 0
	ExitCodeError        = 1
	ExitCodeInvalidUsage = 2
)

func main() {
	var err error

	cmd, cfg, err := config.NewConfig(os.Args)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(ExitCodeInvalidUsage)
	}
	if cfg == nil {
		os.Exit(ExitCodeInvalidUsage)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info(fmt.Sprintf("%s: start", cmd))
	stats, err := internal.Execute(ctx, cmd, cfg)
	if stats != nil {
		stats.Print()
	}
	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(ExitCodeError)
	} else {
		slog.Info(fmt.Sprintf("%s: finish", cmd))
		os.Exit(ExitCodeDone)
	}
}
