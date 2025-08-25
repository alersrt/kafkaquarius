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
	var err error

	cmd, cfg, err := config.NewConfig(os.Args)
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	var app internal.App
	switch cmd {
	case config.CmdMigrate:
		app, err = internal.NewMigrateApp(cfg)
	case config.CmdSearch:
		app, err = internal.NewSearchApp(cfg)
	default:
		slog.Error(fmt.Sprintf("unimplemented"))
		os.Exit(ExitCodeErr)
	}

	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	err = app.Execute()
	if err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(ExitCodeErr)
	}

	os.Exit(ExitCodeDone)
}
