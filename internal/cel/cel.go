package cel

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/araddon/dateparse"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/google/uuid"
)

const (
	varNameSelf       = "self"
	funcNameUuid      = "uuid"
	funcNameNow       = "now"
	funcNameUnmarshal = "unmarshal"
	funcNameMarshal   = "marshal"
)

type Cel struct {
	prog cel.Program
}

func NewCel(expression string) (*Cel, error) {
	env, err := cel.NewEnv(
		cel.Variable(varNameSelf, cel.DynType),
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
		cel.Function("unixSubmilli",
			cel.MemberOverload("timestamp_to_epoch_seconds_with_submillis",
				[]*cel.Type{cel.TimestampType}, cel.DoubleType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					ts, ok := value.Value().(time.Time)
					if !ok {
						return types.NewErr("cel: not a timestamp")
					}
					return types.Double(float64(ts.UnixMilli()) / 1000)
				}),
			),
		),
		cel.Function("unix",
			cel.MemberOverload("timestamp_to_epoch_seconds",
				[]*cel.Type{cel.TimestampType}, cel.IntType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					ts, ok := value.Value().(time.Time)
					if !ok {
						return types.NewErr("cel: not a timestamp")
					}
					return types.Int(ts.Unix())
				}),
			),
		),
		cel.Function("unixMilli",
			cel.MemberOverload("timestamp_to_epoch_milliseconds",
				[]*cel.Type{cel.TimestampType}, cel.IntType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					ts, ok := value.Value().(time.Time)
					if !ok {
						return types.NewErr("cel: not a timestamp")
					}
					return types.Int(ts.UnixMilli())
				}),
			),
		),
		cel.Function(overloads.TypeConvertTimestamp,
			cel.Overload(overloads.StringToTimestamp,
				[]*cel.Type{cel.StringType}, cel.TimestampType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					str, ok := value.Value().(string)
					if !ok {
						return types.NewErr("cel: not a string")
					}
					ts, err := dateparse.ParseAny(str, dateparse.PreferMonthFirst(false))
					if err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.Timestamp{Time: ts}
				}),
			),
		),
		cel.Function(funcNameUnmarshal,
			cel.Overload(funcNameUnmarshal+"_from_bytes",
				[]*cel.Type{cel.BytesType}, cel.DynType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					var dst any
					if err := json.Unmarshal(value.Value().([]byte), &dst); err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.DefaultTypeAdapter.NativeToValue(dst)
				}),
			),
		),
		cel.Function(funcNameMarshal,
			cel.Overload(funcNameUnmarshal+"_to_bytes",
				[]*cel.Type{cel.DynType}, cel.BytesType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					bytes, err := json.Marshal(convert(value.Value()))
					if err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.Bytes(bytes)
				}),
			),
		),
		ext.NativeTypes(reflect.TypeFor[kafka.Message]()),
		ext.Bindings(),
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

func convert(src any) any {
	switch typed := src.(type) {
	case map[ref.Val]ref.Val:
		dst := make(map[string]any)
		for k, v := range typed {
			dst[k.Value().(string)] = convert(v)
		}
		return dst
	case []ref.Val:
		dst := make([]any, len(typed))
		for i, v := range typed {
			dst[i] = convert(v)
		}
		return dst
	case types.Null:
		return nil
	case ref.Val:
		return convert(typed.Value())
	default:
		return typed
	}
}
