package config

import "testing"

func Test(t *testing.T) {
	tests := []struct {
		name   string
		input  []string
		expCmd string
	}{
		{CmdMigrate, []string{CmdMigrate, "--filter-file"}, CmdMigrate},
		{CmdSearch, []string{CmdSearch, "--filter-file"}, CmdSearch},
		{CmdStats, []string{CmdStats, "--filter-file"}, CmdStats},
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
