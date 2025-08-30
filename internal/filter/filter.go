package filter

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/types/known/timestamppb"
	"kafkaquarius/internal/domain"
)

const (
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
		cel.Variable(VarKey, cel.AnyType),
		cel.Variable(VarValue, cel.AnyType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable(VarTimestamp, cel.TimestampType),
		cel.Function(overloads.TypeConvertString, cel.Overload(
			"map_to_string", []*cel.Type{cel.MapType(cel.StringType, cel.AnyType)}, cel.StringType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				b, _ := json.Marshal(value.Value())
				return types.String(b)
			}),
		)),
		cel.OptionalTypes(),
		ext.Regex(),
		ext.Strings(),
		ext.Encoders(),
		ext.Math(),
		ext.Sets(),
		ext.Lists(),
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

func (f *Filter) Eval(msg *kafka.Message) (bool, error) {
	headers := make(map[string]any, len(msg.Headers))
	for _, hdr := range msg.Headers {
		headers[hdr.Key] = domain.Des(hdr.Value)
	}

	data := map[string]any{
		VarKey:       domain.Des(msg.Key),
		VarValue:     domain.Des(msg.Value),
		VarHeaders:   headers,
		VarTimestamp: timestamppb.New(msg.Timestamp),
	}

	eval, _, err := f.prog.Eval(data)
	if err != nil {
		return false, fmt.Errorf("filter: eval: %v", err)
	}
	return eval.Value().(bool), nil
}
