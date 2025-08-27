package daemon

import (
	"context"
	"testing"
	"time"
)

func TestHandleSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	act, _ := HandleSignals(ctx)
	if act != ExitCodeError {
		t.Fatalf("exp=%d, act=%d", ExitCodeError, act)
	}
}
