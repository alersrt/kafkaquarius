package daemon

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

const (
	ExitCodeDone         = 0
	ExitCodeError        = 1
	ExitCodeInvalidUsage = 2
	ExitCodeInterrupt    = 130
	ExitCodeKill         = 137
	ExitCodeTerminate    = 143
)

// HandleSignals handles os signals. Returns exit code and error if any.
func HandleSignals(ctx context.Context, cancel func()) (int, error) {
	signalChan := make(chan os.Signal, 1)
	defer signal.Stop(signalChan)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	for {
		select {
		case s := <-signalChan:
			switch s {
			case os.Interrupt:
				cancel()
				return ExitCodeInterrupt, nil
			case syscall.SIGTERM:
				cancel()
				return ExitCodeTerminate, nil
			}
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return ExitCodeError, err
			} else {
				return ExitCodeDone, nil
			}
		}
	}
}
