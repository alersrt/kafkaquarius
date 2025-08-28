package main

import (
	"context"
	"fmt"
	"kafkaquarius/internal"
	"kafkaquarius/internal/config"
	"kafkaquarius/pkg/daemon"
	"log/slog"
	"os"
)

func main() {
	var err error

	cmd, cfg, err := config.NewConfig(os.Args)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(daemon.ExitCodeInvalidUsage)
	}
	if cfg == nil {
		os.Exit(daemon.ExitCodeInvalidUsage)
	}

	ctx := context.Background()

	slog.Info(fmt.Sprintf("%s: start", cmd))
	stats, err := internal.Execute(ctx, cmd, cfg)
	if stats != nil {
		stats.Print()
	}
	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(daemon.ExitCodeError)
	} else {
		slog.Info(fmt.Sprintf("%s: finish", cmd))
		os.Exit(daemon.ExitCodeDone)
	}
}
