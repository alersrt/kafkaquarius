package internal

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"reflect"
	"time"
)

var (
	VarKey       = "Key"
	VarValue     = "Value"
	VarTimestamp = "Timestamp"
	VarHeaders   = "Headers"
)

type Filter struct {
	prog cel.Program
}

func NewFilter(filter string) (*Filter, error) {
	env, err := cel.NewEnv(
		cel.Variable(VarKey, cel.StringType),
		cel.Variable(VarValue, cel.AnyType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable(VarTimestamp, cel.ObjectType("time.Time")),
		ext.NativeTypes(reflect.TypeFor[time.Time]()),
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

func (f *Filter) Eval(data map[string]any) (bool, error) {
	eval, _, err := f.prog.Eval(data)
	if err != nil {
		return false, fmt.Errorf("filter: eval: %v", err)
	}
	return eval.Value().(bool), nil
}
