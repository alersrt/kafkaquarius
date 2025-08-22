package internal

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"os"
)

var (
	VarKey       = "key"
	VarValue     = "value"
	VarTimestamp = "timestamp"
	VarHeaders   = "headers"
)

type Filter struct {
	prog cel.Program
}

func NewFilter(path string) (*Filter, error) {
	filter, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	env, err := cel.NewEnv(
		cel.Variable(VarKey, cel.AnyType),
		cel.Variable(VarValue, cel.AnyType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable(VarTimestamp, cel.TimestampType),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	ast, iss := env.Compile(string(filter))
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	return &Filter{prog: prog}, nil
}

func (f *Filter) Eval(data map[string]any) (bool, error) {
	eval, _, err := f.prog.Eval(data)
	if err != nil {
		return false, fmt.Errorf("filter: eval: %v", err)
	}
	return eval.Value().(bool), nil
}
