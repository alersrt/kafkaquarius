package cel

import (
	"encoding/json"
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/google/uuid"
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
		cel.Function(funcNameUnbox,
			cel.Overload("unbox_bytes",
				[]*cel.Type{cel.BytesType}, cel.MapType(cel.StringType, cel.DynType),
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					var dst map[string]any
					if err := json.Unmarshal(value.Value().([]byte), &dst); err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.NewStringInterfaceMap(nil, dst)
				}),
			),
			cel.Overload("unbox_string",
				[]*cel.Type{cel.StringType}, cel.MapType(cel.StringType, cel.DynType),
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					var dst map[string]any
					if err := json.Unmarshal([]byte(value.Value().(string)), &dst); err != nil {
						return types.NewErr("cel: %w", err)
					}
					return types.NewStringInterfaceMap(nil, dst)
				}),
			),
		),
		cel.OptionalTypes(),
		ext.Regex(),
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

func (p *Cel) Eval(data any) (any, error) {
	eval, _, err := p.prog.Eval(map[string]any{varNameSelf: data})
	if err != nil {
		return nil, fmt.Errorf("cel: eval: %v", err)
	}

	switch eV := eval.Value().(type) {
	case bool,
		string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64,
		complex64, complex128:
		return eV, nil
	default:
		return convert(eV), nil
	}
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
	case ref.Val:
		return convert(typed.Value())
	default:
		return typed
	}
}
