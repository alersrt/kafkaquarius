package cel

import (
	"fmt"
	"strings"
	"testing"
)

func TestNodeCount(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		expected   int
	}{
		{"literal", "1", 1},
		{"binary", "1 + 2", 3},
		{"map", "{'a': 1, 'b': 2}", 7},
		{"macro", "[1, 2].map(x, x + 1)", 15},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := NodeCount(test.expression)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if count != test.expected {
				t.Errorf("expected %d, got %d", test.expected, count)
			}
		})
	}
}

func TestCheckNodeLimit(t *testing.T) {
	if _, err := CheckNodeLimit("1 + 2", 3); err != nil {
		t.Errorf("%+v", err)
	}
	if _, err := CheckNodeLimit("1 + 2", 2); err == nil {
		t.Error("expected an error")
	}
}

func TestCheckSizeLimit(t *testing.T) {
	if _, err := CheckSizeLimit("1234", 4); err != nil {
		t.Errorf("%+v", err)
	}
	if _, err := CheckSizeLimit("12345", 4); err == nil {
		t.Error("expected an error")
	}
}

func TestSplitSizeLimit(t *testing.T) {
	var builder strings.Builder
	builder.WriteString("{")
	const entries = 500
	for i := 0; i < entries; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "'key%03d': %d", i, i)
	}
	builder.WriteString("}")

	const sizeLimit = 1_000
	parts, err := Split(builder.String(), 1_000_000, sizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 parts, got %d", len(parts))
	}
	for _, part := range parts {
		if _, err := CheckSizeLimit(part, sizeLimit); err != nil {
			t.Errorf("part %q: %+v", snippet(part), err)
		}
	}
}

func TestSplitMap(t *testing.T) {
	testSplit(t, `{'a': 1, 'b': 2, 'c': 3, 'd': 4}`, 4)
}

func TestSplitList(t *testing.T) {
	testSplit(t, `[1, 2, 3, 4, 5]`, 3)
}

func TestSplitBool(t *testing.T) {
	testSplit(t, `a && b && c && d && e`, 3)
}

func TestSplitWithinLimit(t *testing.T) {
	const expression = `{'a': 1}`
	parts, err := Split(expression, 10, DefaultExpressionSizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if parts[0] != expression {
		t.Errorf("expected original expression, got %q", parts[0])
	}
}

func TestSplitUnsupported(t *testing.T) {
	if _, err := Split(`marshal({'a': 1, 'b': 2, 'c': 3})`, 2, DefaultExpressionSizeLimit); err == nil {
		t.Error("expected an error")
	}
}

func TestSplitMixedLogicalOperators(t *testing.T) {
	for _, expression := range []string{
		`a && (b || c || d)`,
		`a || (b && c && d)`,
	} {
		if _, err := Split(expression, 3, DefaultExpressionSizeLimit); err == nil {
			t.Errorf("expected an error for %q", expression)
		}
	}
}

func TestSplitStructFieldEscaping(t *testing.T) {
	testSplit(t, "Msg{`foo-bar`: 1, `baz`: 2}", 4)
}

func TestSplitMacroNodeCount(t *testing.T) {
	const expression = `{ "a": [1,2].map(x, x + 1), "b": [3,4].map(y, y + 1) }`

	count, err := NodeCount(expression)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	parts, err := Split(expression, count, DefaultExpressionSizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 parts, got %d", len(parts))
	}
	for _, part := range parts {
		if _, err := CheckNodeLimit(part, count); err != nil {
			t.Errorf("part %q: %+v", part, err)
		}
	}
}

func TestSplitLargeExpression(t *testing.T) {
	var builder strings.Builder
	builder.WriteString("{")
	const entries = 8000
	for i := 0; i < entries; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "'key%05d': %d", i, i)
	}
	builder.WriteString("}")

	expression := builder.String()
	if len(expression) <= 100_000 {
		t.Fatalf("expected expression larger than the default parser limit, got %d bytes", len(expression))
	}

	const limit = 10_000
	parts, err := Split(expression, limit, DefaultExpressionSizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 parts, got %d", len(parts))
	}
	for _, part := range parts {
		if _, err := CheckNodeLimit(part, limit); err != nil {
			t.Errorf("part %q: %+v", snippet(part), err)
		}
	}
}

func TestSplitNestedExpression(t *testing.T) {
	const expression = `{ "Value": cel.bind(u_value, unmarshal(self.Value), marshal({"a": 1, "b": 2, "c": 3, "d": 4})) }`
	const sizeLimit = 80

	parts, err := Split(expression, 1_000, sizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 parts, got %d", len(parts))
	}
	for _, part := range parts {
		if !strings.Contains(part, `"Value"`) {
			t.Errorf("part %q lost the wrapper", snippet(part))
		}
		if err := sCheckPart(part, 1_000, sizeLimit); err != nil {
			t.Errorf("part %q: %+v", snippet(part), err)
		}
	}
}

func TestSplitNestedEntries(t *testing.T) {
	var builder strings.Builder
	builder.WriteString(`{ "Value": marshal({`)
	const outer = 2
	const inner = 20
	for i := 0; i < outer; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, `"outer%d": {`, i)
		for j := 0; j < inner; j++ {
			if j > 0 {
				builder.WriteString(", ")
			}
			fmt.Fprintf(&builder, `"key%02d": "value-%02d-xxxxxxxx"`, j, j)
		}
		builder.WriteString("}")
	}
	builder.WriteString(`}) }`)

	const sizeLimit = 180
	parts, err := Split(builder.String(), 1_000, sizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 4 {
		t.Fatalf("expected at least 4 parts, got %d", len(parts))
	}
	for _, part := range parts {
		if !strings.Contains(part, `"Value"`) {
			t.Errorf("part %q lost the wrapper", snippet(part))
		}
		if err := sCheckPart(part, 1_000, sizeLimit); err != nil {
			t.Errorf("part %q: %+v", snippet(part), err)
		}
	}
}

func sCheckPart(expression string, nodeLimit, sizeLimit int) error {
	if _, err := CheckNodeLimit(expression, nodeLimit); err != nil {
		return err
	}
	_, err := CheckSizeLimit(expression, sizeLimit)
	return err
}

func testSplit(t *testing.T, expression string, limit int) {
	t.Helper()

	parts, err := Split(expression, limit, DefaultExpressionSizeLimit)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 parts, got %d", len(parts))
	}
	for _, part := range parts {
		count, err := CheckNodeLimit(part, limit)
		if err != nil {
			t.Errorf("part %q: %+v", part, err)
			continue
		}
		if count > limit {
			t.Errorf("part %q has %d nodes, limit %d", part, count, limit)
		}
	}
}
