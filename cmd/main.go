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
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(daemon.ExitCodeInvalidUsage)
	}

	ctx, cancel := context.WithCancel(context.Background())

	switch cmd {
	case config.CmdMigrate:
		go internal.Migrate(ctx, cfg)
	case config.CmdSearch:
		go internal.Search(ctx, cfg)
	}

	if code, err := daemon.HandleSignals(ctx, cancel); err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(code)
	}

	os.Exit(daemon.ExitCodeDone)
}
