package main

import (
	"fmt"
	"kafkaquarius/internal"
	"kafkaquarius/internal/config"
	"log/slog"
	"os"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

func main() {
	_, cfg, err := config.NewConfig(os.Args)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	_, err = internal.NewApp(cfg)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	os.Exit(ExitCodeDone)
}
