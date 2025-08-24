package internal

import (
	"fmt"
	"github.com/google/cel-go/cel"
)

type Filter struct {
	prog cel.Program
}

func NewFilter(filter string) (*Filter, error) {
	env, err := cel.NewEnv(
		cel.Variable(VarKey, cel.AnyType),
		cel.Variable(VarValue, cel.AnyType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable(VarTimestamp, cel.TimestampType),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	ast, iss := env.Compile(filter)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("filter: new: %v", iss.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("filter: new: %v", err)
	}

	return &Filter{prog: prog}, nil
}

func (f *Filter) Eval(data any) (bool, error) {
	eval, _, err := f.prog.Eval(data)
	if err != nil {
		return false, fmt.Errorf("filter: eval: %v", err)
	}
	return eval.Value().(bool), nil
}
