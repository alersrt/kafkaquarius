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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info(fmt.Sprintf("%s: starting", cmd))

	app, err := internal.NewApp(cmd, cfg)
	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(ExitCodeError)
	}
	defer app.Close()

	slog.Info(fmt.Sprintf("%s: started", cmd))

	err = app.Execute(ctx)

	slog.Info(fmt.Sprintf("%s: %s", cmd, app.Stats().FormattedString(`statistic:
Time:	{{ .Time }}
Total:	{{ .Total }}
Found:	{{ .Found }}
Proc:	{{ .Proc }}
Errors:	{{ .Errors }}
Offsets:	{{ .Offsets }}
`)))

	if err != nil {
		slog.Error(fmt.Sprintf("%s: %v", cmd, err))
		os.Exit(ExitCodeError)
	} else {
		slog.Info(fmt.Sprintf("%s: finished", cmd))
		os.Exit(ExitCodeDone)
	}
}
