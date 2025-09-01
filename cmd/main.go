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

	slog.Info(fmt.Sprintf("%s: starting", cmd))

	stop := make(chan byte)
	go printStarting(ctx, stop)

	app, err := internal.NewApp(cmd, cfg)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(ExitCodeError)
	}
	defer app.Close()
	close(stop)

	slog.Info(fmt.Sprintf("%s: started", cmd))

	go printStats(ctx, app)

	err = app.Execute(ctx)
	fmt.Printf("\r%s\n", app.Stats().FormattedString())

	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(ExitCodeError)
	} else {
		slog.Info(fmt.Sprintf("%s: finished", cmd))
		os.Exit(ExitCodeDone)
	}
}

func printStarting(ctx context.Context, stop chan byte) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	startTs := time.Now()
	for {
		select {
		case <-stop:
			fmt.Printf("\n")
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\rStarting:\t%s", time.Since(startTs).Truncate(time.Millisecond))
		}
	}
}

func printStats(ctx context.Context, app *internal.App) {
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
