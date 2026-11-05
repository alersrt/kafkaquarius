package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizePath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("home directory is unavailable: %v", err)
	}

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"current", ".", "."},
		{"cleans relative", "./a/../b", "b"},
		{"keeps parent", "../x", "../x"},
		{"cleans absolute", "/tmp/./x/../y", "/tmp/y"},
		{"home", "~", filepath.Clean(home)},
		{"home subdir", "~/tmp", filepath.Join(home, "tmp")},
		{"home with parent", "~/tmp/../out", filepath.Join(home, "out")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizePath(test.in); got != test.want {
				t.Errorf("normalizePath(%q) = %q, want %q", test.in, got, test.want)
			}
		})
	}
}

func Test(t *testing.T) {
	tests := []struct {
		name   string
		input  []string
		expCmd string
	}{
		{CmdMigrate, []string{"app", CmdMigrate,
			"--filter-file=/path/to/file",
			"--source-broker=localhost:9092", "--source-topic=test-topic", "--consumer-group=kafkaquarius",
			"--leeroy=true",
		}, CmdMigrate},
		{CmdSearch, []string{"app", CmdSearch,
			"--filter-file=/path/to/file",
			"--source-broker=localhost:9092", "--source-topic=test-topic", "--consumer-group=kafkaquarius",
		}, CmdSearch},
		{CmdCheck, []string{"app", CmdCheck,
			"--file=/path/to/file", "--expr-node-limit=100",
		}, CmdCheck},
		{CmdSplit, []string{"app", CmdSplit,
			"--file=/path/to/file", "--expr-node-limit=100", "--output-dir=/tmp",
		}, CmdSplit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actCmd, cfg, err := NewConfig(test.input)
			if err != nil {
				t.Errorf("error: %+v", err)
			}
			if cfg == nil {
				t.Errorf("nil result")
			}
			if actCmd != test.expCmd {
				t.Errorf("cmd mismatch: exp=%s, act=%s", test.expCmd, actCmd)
			}
		})
	}
}
