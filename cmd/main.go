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
	"time"
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

	app, err := internal.NewApp(cmd, cfg)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(ExitCodeError)
	}
	defer app.Close()

	slog.Info(fmt.Sprintf("%s: start", cmd))

	go print(ctx, app)

	err = app.Execute(ctx)
	fmt.Printf("\r%s\n", app.Stats().FormattedString())

	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(ExitCodeError)
	} else {
		slog.Info(fmt.Sprintf("%s: finish", cmd))
		os.Exit(ExitCodeDone)
	}
}

func print(ctx context.Context, app *internal.App) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\r%s", app.Stats().FormattedString())
		}
	}
}
