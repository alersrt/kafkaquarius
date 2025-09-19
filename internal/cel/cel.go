package cel

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/google/uuid"
	"kafkaquarius/internal/domain"
	"reflect"
	"time"
)

const (
	varNameSelf   = "self"
	funcNameUuid  = "uuid"
	funcNameNow   = "now"
	funcNameUnbox = "unbox"
)

type Cel struct {
	prog cel.Program
}

func NewCel(expression string) (*Cel, error) {
	env, err := cel.NewEnv(
		cel.Variable(varNameSelf, cel.MapType(cel.StringType, cel.DynType)),
		cel.Function(overloads.TypeConvertString, cel.Overload(
			"map_to_string", []*cel.Type{cel.MapType(cel.StringType, cel.DynType)}, cel.StringType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				b, err := json.Marshal(value.Value())
				if err != nil {
					return types.NewErr("cel: %w", err)
				}
				return types.String(b)
			}),
		)),
		cel.Function(funcNameUuid,
			cel.Overload("uuid_random",
				nil, cel.StringType,
				cel.FunctionBinding(func(values ...ref.Val) ref.Val {
					return types.String(uuid.NewString())
				}),
			),
			cel.Overload("bytes_to_uuid",
				[]*cel.Type{cel.BytesType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					parsed, err := uuid.ParseBytes(value.Value().([]byte))
					if err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.String(parsed.String())
				}),
			),
			cel.Overload("string_to_uuid",
				[]*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					parsed, err := uuid.Parse(value.Value().(string))
					if err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.String(parsed.String())
				}),
			),
		),
		cel.Function(funcNameNow,
			cel.Overload("timestamp_now",
				nil, cel.TimestampType,
				cel.FunctionBinding(func(values ...ref.Val) ref.Val {
					return types.Timestamp{Time: time.Now()}
				}),
			),
		),
		cel.Function(funcNameUnbox,
			cel.Overload("unbox_bytes",
				[]*cel.Type{cel.BytesType}, cel.MapType(cel.StringType, cel.DynType),
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					dst := make(map[string]any)
					if err := json.Unmarshal(value.Value().([]byte), &dst); err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.DefaultTypeAdapter.NativeToValue(dst)
				}),
			),
			cel.Overload("unbox_string",
				[]*cel.Type{cel.StringType}, cel.MapType(cel.StringType, cel.DynType),
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					dst := make(map[string]any)
					if err := json.Unmarshal([]byte(value.Value().(string)), &dst); err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.DefaultTypeAdapter.NativeToValue(dst)
				}),
			),
		),
		ext.NativeTypes(
			reflect.TypeFor[kafka.Message](),
			reflect.TypeFor[domain.MessageWithAny](),
			reflect.TypeFor[domain.MessageWithStrings](),
		),
		ext.Strings(),
		ext.Encoders(),
		ext.Math(),
		ext.Sets(),
		ext.Lists(),
		ext.TwoVarComprehensions(),
	)
	if err != nil {
		return nil, fmt.Errorf("cel: init: %v", err)
	}

	ast, iss := env.Compile(expression)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("cel: init: %v", iss.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("cel: init: %v", err)
	}

	return &Cel{prog: prog}, nil
}

func (p *Cel) Eval(data any, typeDesc reflect.Type) (any, error) {
	eval, _, err := p.prog.Eval(map[string]any{varNameSelf: data})
	if err != nil {
		return nil, fmt.Errorf("cel: eval: %v", err)
	}

	return eval.ConvertToNative(typeDesc)
}
