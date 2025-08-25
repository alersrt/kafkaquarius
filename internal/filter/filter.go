package filter

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/types/known/timestamppb"
	"kafkaquarius/internal/mapper"
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
		headers[hdr.Key] = mapper.Des(hdr.Value)
	}

	data := map[string]any{
		VarKey:       mapper.Des(msg.Key),
		VarValue:     mapper.Des(msg.Value),
		VarHeaders:   headers,
		VarTimestamp: timestamppb.New(msg.Timestamp),
	}

	eval, _, err := f.prog.Eval(data)
	if err != nil {
		return false, fmt.Errorf("filter: eval: %v", err)
	}
	return eval.Value().(bool), nil
}
