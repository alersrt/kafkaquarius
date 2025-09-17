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
	ExitCodeInterrupt    = 128 + int(syscall.SIGINT)
)

func main() {
	var err error
	cmd, cfg, err := config.NewConfig(os.Args)
	if err != nil || cfg == nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(ExitCodeInvalidUsage)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	stop := make(chan error)
	defer close(stop)

	app := new(internal.App)
	go func() {
		var err error
		defer func() { stop <- err }()
		slog.Info(fmt.Sprintf("%s: starting", cmd))

		err = app.Init(cmd, cfg)
		if err != nil {
			return
		}
		defer app.Close()

		slog.Info(fmt.Sprintf("%s: started", cmd))
		err = app.Execute(ctx, cmd)
	}()

	select {
	case err = <-stop:
	case <-ctx.Done():
	}

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
	} else if ctx.Err() != nil {
		slog.Error(fmt.Sprintf("%s: interrupt", cmd))
		os.Exit(ExitCodeInterrupt)
	} else {
		slog.Info(fmt.Sprintf("%s: finished", cmd))
		os.Exit(ExitCodeDone)
	}
}
