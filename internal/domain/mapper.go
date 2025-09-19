package domain

import (
	"encoding/json"
)

func Des(value []byte) any {
	var umars map[string]any
	err := json.Unmarshal(value, &umars)
	if err != nil {
		return string(value)
	} else {
		return umars
	}
}
