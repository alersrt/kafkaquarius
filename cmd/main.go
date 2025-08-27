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

	ctx := context.Background()

	switch cmd {
	case config.CmdMigrate:
		go func() {
			slog.Info("migrate: start")
			stat, err := internal.Migrate(ctx, cfg)
			if err != nil {
				slog.Error(fmt.Sprintf("migrate: %v", err))
			}
			slog.Info("migrate: finish")
			stat.Print()
		}()
	case config.CmdSearch:
		go func() {
			slog.Info("search: start")
			stat, err := internal.Search(ctx, cfg)
			if err != nil {
				slog.Error(fmt.Sprintf("search: %v", err))
			}
			slog.Info("search: finish")
			stat.Print()
		}()
	}

	if code, err := daemon.HandleSignals(ctx); err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(code)
	}

	os.Exit(daemon.ExitCodeDone)
}
