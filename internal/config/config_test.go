package config

import "testing"

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
