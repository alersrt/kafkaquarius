package internal

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"
	"reflect"
	"time"
)

var (
	VarKey       = "Key"
	VarValue     = "Value"
	VarTimestamp = "Ts"
	VarHeaders   = "Headers"
)

type Filter struct {
	prog cel.Program
}

func NewFilter(filter string) (*Filter, error) {
	TimeType := cel.ObjectType("time.Time", traits.AdderType, traits.ComparerType, traits.ReceiverType, traits.SubtractorType)
	env, err := cel.NewEnv(
		cel.Variable(VarKey, cel.StringType),
		cel.Variable(VarValue, cel.AnyType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable(VarTimestamp, TimeType),
		ext.NativeTypes(reflect.TypeFor[time.Time]()),
		cel.Function(
			"time",
			cel.Overload("time_to_time", []*cel.Type{TimeType}, TimeType),
			cel.Overload("int_to_time", []*cel.Type{cel.IntType}, TimeType),
			cel.Overload("string_to_time", []*cel.Type{cel.StringType}, TimeType),
		),
		cel.Function(
			operators.Less,
			cel.Overload("less_time", []*cel.Type{TimeType, TimeType}, cel.BoolType),
		),
		cel.Function(
			operators.LessEquals,
			cel.Overload("less_equals_time", []*cel.Type{TimeType, TimeType}, cel.BoolType),
		),
		cel.Function(
			operators.Greater,
			cel.Overload("greater_time", []*cel.Type{TimeType, TimeType}, cel.BoolType),
		),
		cel.Function(
			operators.GreaterEquals,
			cel.Overload("greater_equals_time", []*cel.Type{TimeType, TimeType}, cel.BoolType),
		),
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
