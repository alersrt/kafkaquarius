package cel

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	celgo "github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

// DefaultExpressionSizeLimit matches the cel-go parser default for the number
// of code points in an expression.
const DefaultExpressionSizeLimit = 100_000

const (
	opAnd = "_&&_"
	opOr  = "_||_"
)

var fieldNamePattern = regexp.MustCompile(`^[A-Za-z_][0-9A-Za-z_]*$`)

// NodeCount returns the number of expression nodes, including nodes created by
// macro expansion. The counting matches cel.ExpressionNodeLimit semantics.
func NodeCount(expression string) (int, error) {
	env, err := newParseEnv(celgo.ExpressionNodeLimit(-1))
	if err != nil {
		return 0, err
	}
	return countWith(env, expression)
}

// CheckNodeLimit verifies that the expression fits into the provided node limit.
func CheckNodeLimit(expression string, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("cel: expression node limit must be positive")
	}

	limitEnv, err := newParseEnv(celgo.ExpressionNodeLimit(limit))
	if err != nil {
		return 0, err
	}
	countEnv, err := newParseEnv(celgo.ExpressionNodeLimit(-1))
	if err != nil {
		return 0, err
	}

	count, fits, err := fit(limitEnv, countEnv, expression, limit)
	if err != nil {
		return 0, err
	}
	if fits {
		return count, nil
	}

	if count == 0 {
		count, _ = countWith(countEnv, expression)
	}
	return count, fmt.Errorf("cel: expression node count %d exceeds limit %d", count, limit)
}

// CheckSizeLimit verifies that the expression code point count fits into the
// provided limit. cel-go applies this limit in addition to the node limit.
func CheckSizeLimit(expression string, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("cel: expression size limit must be positive")
	}

	size := utf8.RuneCountInString(expression)
	if size > limit {
		return size, fmt.Errorf("cel: expression size %d exceeds limit %d", size, limit)
	}
	return size, nil
}

// Split breaks the expression into independent parts so that each part fits
// into the provided node and code point limits. Top-level map entries, list
// elements and boolean conjunctions/disjunctions are used as split boundaries.
func Split(expression string, nodeLimit int, sizeLimits ...int) ([]string, error) {
	if nodeLimit <= 0 {
		return nil, fmt.Errorf("cel: expression node limit must be positive")
	}

	sizeLimit := DefaultExpressionSizeLimit
	if len(sizeLimits) > 0 {
		sizeLimit = sizeLimits[0]
	}
	if sizeLimit <= 0 {
		return nil, fmt.Errorf("cel: expression size limit must be positive")
	}

	limitEnv, err := newParseEnv(celgo.ExpressionNodeLimit(nodeLimit))
	if err != nil {
		return nil, err
	}
	countEnv, err := newParseEnv(celgo.ExpressionNodeLimit(-1))
	if err != nil {
		return nil, err
	}

	s := &splitter{
		limit:     nodeLimit,
		sizeLimit: sizeLimit,
		limitEnv:  limitEnv,
		countEnv:  countEnv,
	}

	parts, err := s.split(expression)
	if err != nil {
		return nil, err
	}

	for _, part := range parts {
		if err := s.checkPart(part); err != nil {
			return nil, err
		}
	}

	return parts, nil
}

type splitter struct {
	limit     int
	sizeLimit int
	limitEnv  *celgo.Env
	countEnv  *celgo.Env
}

func (s *splitter) split(expression string) ([]string, error) {
	_, fits, err := s.fit(expression)
	if err != nil {
		return nil, err
	}
	if fits {
		return []string{expression}, nil
	}

	ast, err := parseWith(s.countEnv, expression)
	if err != nil {
		return nil, err
	}

	count := celast.NodeCount(ast.NativeRep())
	size := utf8.RuneCountInString(expression)
	root := ast.NativeRep().Expr()
	var parts []string
	fallback := false

	switch root.Kind() {
	case celast.CallKind:
		call := root.AsCall()
		if !call.IsMemberFunction() && (call.FunctionName() == opAnd || call.FunctionName() == opOr) {
			parts, err = s.splitBool(ast, call.FunctionName())
		} else {
			fallback = true
		}
	case celast.MapKind:
		parts, err = s.splitMap(ast, root.AsMap())
	case celast.StructKind:
		parts, err = s.splitStruct(ast, root.AsStruct())
	case celast.ListKind:
		parts, err = s.splitList(ast, root.AsList())
	default:
		fallback = true
	}
	if err == nil && parts != nil {
		return parts, nil
	}

	if fallback {
		nested, found, nestedErr := s.splitNested(expression, ast)
		if nestedErr != nil {
			return nil, nestedErr
		}
		if found {
			return nested, nil
		}
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf(
		"cel: expression has %d nodes and %d code points and cannot be split below limits %d nodes / %d code points at top-level %s",
		count, size, s.limit, s.sizeLimit, kindName(root.Kind()),
	)
}

// splitNested locates the largest splittable expression inside a wrapper (for
// example, a large map passed to marshal), splits it and re-wraps every part.
func (s *splitter) splitNested(expression string, ast *celgo.Ast) ([]string, bool, error) {
	info := ast.NativeRep().SourceInfo()
	_, start, stop, found := findSplitCandidate(ast.NativeRep().Expr(), info, expression)
	if !found {
		return nil, false, nil
	}

	nodeText := expression[start:stop]
	prefix := expression[:start]
	suffix := expression[stop:]

	placeholder := prefix + "0" + suffix
	placeholderCount, err := s.count(placeholder)
	if err != nil {
		return nil, false, err
	}
	nodeLimit := s.limit - (placeholderCount - 1)
	sizeLimit := s.sizeLimit - (utf8.RuneCountInString(placeholder) - 1)
	if nodeLimit <= 0 || sizeLimit <= 0 {
		return nil, false, fmt.Errorf(
			"cel: expression wrapper leaves no room for the nested expression at limits %d nodes / %d code points",
			s.limit, s.sizeLimit,
		)
	}

	child := &splitter{
		limit:     nodeLimit,
		sizeLimit: sizeLimit,
		limitEnv:  s.limitEnv,
		countEnv:  s.countEnv,
	}
	parts, err := child.split(nodeText)
	if err != nil {
		return nil, false, err
	}

	result := make([]string, 0, len(parts))
	for _, part := range parts {
		wrapped := prefix + part + suffix
		if err := s.checkPart(wrapped); err != nil {
			return nil, false, err
		}
		result = append(result, wrapped)
	}
	return result, true, nil
}

func findSplitCandidate(
	root celast.Expr,
	info *celast.SourceInfo,
	expression string,
) (celast.Expr, int, int, bool) {
	var (
		best      celast.Expr
		bestStart int
		bestStop  int
		bestSpan  int
	)

	var visit func(celast.Expr)
	visit = func(expr celast.Expr) {
		if expr.ID() != root.ID() && isSplittable(expr) {
			if start, stop, ok := candidateSpan(expression, info, expr); ok && stop-start > bestSpan {
				best = expr
				bestStart = start
				bestStop = stop
				bestSpan = stop - start
			}
		}
		for _, child := range childExpressions(expr) {
			visit(child)
		}
	}
	visit(root)

	return best, bestStart, bestStop, best != nil
}

func candidateSpan(expression string, info *celast.SourceInfo, expr celast.Expr) (int, int, bool) {
	offset, ok := info.GetOffsetRange(expr.ID())
	if !ok {
		return 0, 0, false
	}
	start := int(offset.Start)
	if start < 0 || start >= len(expression) {
		return 0, 0, false
	}

	switch expr.Kind() {
	case celast.MapKind:
		if expression[start] == '{' {
			stop, ok := scanBalanced(expression, start)
			return start, stop, ok
		}
	case celast.ListKind:
		if expression[start] == '[' {
			stop, ok := scanBalanced(expression, start)
			return start, stop, ok
		}
	case celast.StructKind:
		if expression[start] == '{' {
			nameStart := scanTypeNameStart(expression, start)
			stop, ok := scanBalanced(expression, start)
			return nameStart, stop, ok
		}
	}
	return 0, 0, false
}

// scanBalanced returns the position just after the closing delimiter that
// matches the opening delimiter at start.
func scanBalanced(expression string, start int) (int, bool) {
	stack := make([]byte, 0, 8)
	for i := start; i < len(expression); i++ {
		switch expression[i] {
		case '\'', '"':
			next, ok := scanString(expression, i)
			if !ok {
				return 0, false
			}
			i = next - 1
		case '/':
			if i+1 < len(expression) && expression[i+1] == '/' {
				for i < len(expression) && expression[i] != '\n' {
					i++
				}
			}
		case '{', '[', '(':
			stack = append(stack, expression[i])
		case '}', ']', ')':
			if len(stack) == 0 {
				return 0, false
			}
			open := stack[len(stack)-1]
			if !matches(open, expression[i]) {
				return 0, false
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}

func matches(open, close byte) bool {
	return open == '{' && close == '}' ||
		open == '[' && close == ']' ||
		open == '(' && close == ')'
}

func scanString(expression string, start int) (int, bool) {
	quote := expression[start]
	raw := start > 0 && (expression[start-1] == 'r' || expression[start-1] == 'R')

	if start+2 < len(expression) && expression[start+1] == quote && expression[start+2] == quote {
		for i := start + 3; i+2 < len(expression); i++ {
			if !raw && expression[i] == '\\' {
				i++
				continue
			}
			if expression[i] == quote && expression[i+1] == quote && expression[i+2] == quote {
				return i + 3, true
			}
		}
		return 0, false
	}

	for i := start + 1; i < len(expression); i++ {
		switch expression[i] {
		case '\\':
			if !raw {
				i++
			}
		case quote:
			return i + 1, true
		}
	}
	return 0, false
}

func scanTypeNameStart(expression string, brace int) int {
	start := brace
	for start > 0 {
		c := expression[start-1]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' {
			start--
			continue
		}
		break
	}
	return start
}

func isSplittable(expr celast.Expr) bool {
	switch expr.Kind() {
	case celast.MapKind:
		return expr.AsMap().Size() > 1
	case celast.StructKind:
		return len(expr.AsStruct().Fields()) > 1
	case celast.ListKind:
		return expr.AsList().Size() > 1
	case celast.CallKind:
		call := expr.AsCall()
		if call.IsMemberFunction() {
			return false
		}
		if call.FunctionName() != opAnd && call.FunctionName() != opOr {
			return false
		}
		return len(call.Args()) > 1
	default:
		return false
	}
}

func childExpressions(expr celast.Expr) []celast.Expr {
	switch expr.Kind() {
	case celast.CallKind:
		call := expr.AsCall()
		children := make([]celast.Expr, 0, len(call.Args())+1)
		if call.IsMemberFunction() {
			children = append(children, call.Target())
		}
		return append(children, call.Args()...)
	case celast.ListKind:
		return expr.AsList().Elements()
	case celast.MapKind:
		entries := expr.AsMap().Entries()
		children := make([]celast.Expr, 0, len(entries)*2)
		for _, entry := range entries {
			mapEntry := entry.AsMapEntry()
			children = append(children, mapEntry.Key(), mapEntry.Value())
		}
		return children
	case celast.StructKind:
		fields := expr.AsStruct().Fields()
		children := make([]celast.Expr, 0, len(fields))
		for _, field := range fields {
			children = append(children, field.AsStructField().Value())
		}
		return children
	case celast.SelectKind:
		return []celast.Expr{expr.AsSelect().Operand()}
	case celast.ComprehensionKind:
		comprehension := expr.AsComprehension()
		return []celast.Expr{
			comprehension.IterRange(),
			comprehension.AccuInit(),
			comprehension.LoopCondition(),
			comprehension.LoopStep(),
			comprehension.Result(),
		}
	default:
		return nil
	}
}

func (s *splitter) splitBool(ast *celgo.Ast, op string) ([]string, error) {
	var operands []celast.Expr
	flattenBool(ast.NativeRep().Expr(), op, &operands)

	parts := make([]string, 0, len(operands))
	for _, operand := range operands {
		text, err := expressionText(ast, operand)
		if err != nil {
			return nil, err
		}

		_, fits, err := s.fit(text)
		if err != nil {
			return nil, err
		}
		if !fits {
			operandAst, err := parseWith(s.countEnv, text)
			if err != nil {
				return nil, err
			}
			operandOp, isLogical := logicalFunction(operandAst.NativeRep().Expr())
			if isLogical && operandOp != op {
				return nil, fmt.Errorf(
					"cel: operand %q uses a different logical operator and cannot be split at limit %d",
					snippet(text), s.limit,
				)
			}
			if !isLogical {
				return nil, fmt.Errorf(
					"cel: operand %q cannot be split below limit %d",
					snippet(text), s.limit,
				)
			}
		}

		subParts, err := s.split(text)
		if err != nil {
			return nil, err
		}
		parts = append(parts, subParts...)
	}

	costs := make([]int, len(parts))
	for i, part := range parts {
		count, err := s.count(part)
		if err != nil {
			return nil, err
		}
		costs[i] = count
	}

	symbol := "&&"
	if op == opOr {
		symbol = "||"
	}

	return s.buildGroups(parts, costs, func(size int) int {
		if size < 2 {
			return 0
		}
		return 1
	}, func(items []string) string {
		return "(" + strings.Join(items, ") "+symbol+" (") + ")"
	})
}

func (s *splitter) splitMap(ast *celgo.Ast, expr celast.MapExpr) ([]string, error) {
	type mapEntry struct {
		key      string
		value    string
		optional bool
	}

	entries := make([]mapEntry, 0, expr.Size())
	texts := make([]string, 0, expr.Size())
	for _, rawEntry := range expr.Entries() {
		entry := rawEntry.AsMapEntry()
		key, err := expressionText(ast, entry.Key())
		if err != nil {
			return nil, err
		}
		value, err := expressionText(ast, entry.Value())
		if err != nil {
			return nil, err
		}
		text := key + ": " + value
		if entry.IsOptional() {
			text = "?" + text
		}
		entries = append(entries, mapEntry{
			key:      key,
			value:    value,
			optional: entry.IsOptional(),
		})
		texts = append(texts, text)
	}

	build := func(items []string) string {
		return "{ " + strings.Join(items, ", ") + " }"
	}

	costs, err := s.containerCosts(texts, build)
	if err != nil {
		return nil, err
	}

	var result []string
	var normal []string
	var normalCosts []int
	for i, entry := range entries {
		single := build([]string{texts[i]})
		if costs[i]+1 <= s.limit && utf8.RuneCountInString(single) <= s.sizeLimit {
			normal = append(normal, texts[i])
			normalCosts = append(normalCosts, costs[i])
			continue
		}

		parts, err := s.splitWrappedValue(entry.value, func(part string) string {
			text := entry.key + ": " + part
			if entry.optional {
				text = "?" + text
			}
			return build([]string{text})
		})
		if err != nil {
			return nil, fmt.Errorf("cel: map entry %q: %w", snippet(entry.key), err)
		}
		result = append(result, parts...)
	}

	if len(normal) > 0 {
		groups, err := s.buildGroups(normal, normalCosts, func(int) int { return 1 }, build)
		if err != nil {
			return nil, err
		}
		result = append(result, groups...)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("cel: nothing to split")
	}
	return result, nil
}

func (s *splitter) splitStruct(ast *celgo.Ast, expr celast.StructExpr) ([]string, error) {
	type structField struct {
		name     string
		value    string
		optional bool
	}

	fields := make([]structField, 0, len(expr.Fields()))
	texts := make([]string, 0, len(expr.Fields()))
	for _, rawField := range expr.Fields() {
		field := rawField.AsStructField()
		value, err := expressionText(ast, field.Value())
		if err != nil {
			return nil, err
		}
		name := quoteFieldName(field.Name())
		text := name + ": " + value
		if field.IsOptional() {
			text = "?" + text
		}
		fields = append(fields, structField{
			name:     name,
			value:    value,
			optional: field.IsOptional(),
		})
		texts = append(texts, text)
	}

	typeName := expr.TypeName()
	build := func(items []string) string {
		return typeName + "{ " + strings.Join(items, ", ") + " }"
	}

	costs, err := s.containerCosts(texts, build)
	if err != nil {
		return nil, err
	}

	var result []string
	var normal []string
	var normalCosts []int
	for i, field := range fields {
		single := build([]string{texts[i]})
		if costs[i]+1 <= s.limit && utf8.RuneCountInString(single) <= s.sizeLimit {
			normal = append(normal, texts[i])
			normalCosts = append(normalCosts, costs[i])
			continue
		}

		parts, err := s.splitWrappedValue(field.value, func(part string) string {
			text := field.name + ": " + part
			if field.optional {
				text = "?" + text
			}
			return build([]string{text})
		})
		if err != nil {
			return nil, fmt.Errorf("cel: struct field %q: %w", snippet(field.name), err)
		}
		result = append(result, parts...)
	}

	if len(normal) > 0 {
		groups, err := s.buildGroups(normal, normalCosts, func(int) int { return 1 }, build)
		if err != nil {
			return nil, err
		}
		result = append(result, groups...)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("cel: nothing to split")
	}
	return result, nil
}

func (s *splitter) splitList(ast *celgo.Ast, expr celast.ListExpr) ([]string, error) {
	type listElement struct {
		text     string
		value    string
		optional bool
	}

	elements := make([]listElement, 0, expr.Size())
	for index, exprElement := range expr.Elements() {
		value, err := expressionText(ast, exprElement)
		if err != nil {
			return nil, err
		}
		text := value
		optional := expr.IsOptional(int32(index))
		if optional {
			text = "?" + text
		}
		elements = append(elements, listElement{
			text:     text,
			value:    value,
			optional: optional,
		})
	}

	costs := make([]int, len(elements))
	texts := make([]string, len(elements))
	for i, element := range elements {
		count, err := s.count(element.text)
		if err != nil {
			return nil, err
		}
		costs[i] = count
		texts[i] = element.text
	}

	build := func(items []string) string {
		return "[" + strings.Join(items, ", ") + "]"
	}

	var result []string
	var normal []string
	var normalCosts []int
	for i, element := range elements {
		if costs[i]+1 <= s.limit && utf8.RuneCountInString(build([]string{texts[i]})) <= s.sizeLimit {
			normal = append(normal, texts[i])
			normalCosts = append(normalCosts, costs[i])
			continue
		}

		parts, err := s.splitWrappedValue(element.value, func(part string) string {
			if element.optional {
				part = "?" + part
			}
			return build([]string{part})
		})
		if err != nil {
			return nil, err
		}
		result = append(result, parts...)
	}

	if len(normal) > 0 {
		groups, err := s.buildGroups(normal, normalCosts, func(int) int { return 1 }, build)
		if err != nil {
			return nil, err
		}
		result = append(result, groups...)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("cel: nothing to split")
	}
	return result, nil
}

func (s *splitter) splitWrappedValue(value string, wrap func(string) string) ([]string, error) {
	placeholder := wrap("0")
	count, err := s.count(placeholder)
	if err != nil {
		return nil, err
	}

	nodeLimit := s.limit - (count - 1)
	sizeLimit := s.sizeLimit - (utf8.RuneCountInString(placeholder) - 1)
	if nodeLimit <= 0 || sizeLimit <= 0 {
		return nil, fmt.Errorf(
			"cel: expression wrapper leaves no room for the nested expression at limits %d nodes / %d code points",
			s.limit, s.sizeLimit,
		)
	}

	child := &splitter{
		limit:     nodeLimit,
		sizeLimit: sizeLimit,
		limitEnv:  s.limitEnv,
		countEnv:  s.countEnv,
	}
	parts, err := child.split(value)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(parts))
	for _, part := range parts {
		wrapped := wrap(part)
		if err := s.checkPart(wrapped); err != nil {
			return nil, err
		}
		result = append(result, wrapped)
	}
	return result, nil
}

func (s *splitter) containerCosts(items []string, build func([]string) string) ([]int, error) {
	const overhead = 1

	costs := make([]int, len(items))
	for i, item := range items {
		count, err := s.count(build([]string{item}))
		if err != nil {
			return nil, err
		}
		costs[i] = count - overhead
		if costs[i] < 0 {
			costs[i] = 0
		}
	}
	return costs, nil
}

func (s *splitter) buildGroups(
	items []string,
	costs []int,
	overhead func(size int) int,
	build func([]string) string,
) ([]string, error) {
	// The estimate only needs to be close enough to avoid putting everything
	// into one huge group. Exact checks below handle the remaining cases.
	const sizeOverhead = 16

	var groups [][]string
	var current []string
	currentCost := 0
	currentSize := 0

	for i, item := range items {
		itemSize := utf8.RuneCountInString(item)
		candidateCost := currentCost + costs[i] + overhead(len(current)+1)
		candidateSize := currentSize + itemSize + sizeOverhead
		if candidateCost <= s.limit && candidateSize <= s.sizeLimit {
			current = append(current, item)
			currentCost += costs[i]
			currentSize = candidateSize
			continue
		}

		if len(current) > 0 {
			groups = append(groups, current)
		}
		current = []string{item}
		currentCost = costs[i]
		currentSize = itemSize
	}

	if len(current) > 0 {
		groups = append(groups, current)
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("cel: nothing to split")
	}

	result := make([]string, 0, len(groups))
	for _, group := range groups {
		expression := build(group)
		_, fits, err := s.fit(expression)
		if err != nil {
			return nil, err
		}
		if fits {
			result = append(result, expression)
			continue
		}

		// The incremental estimate can be too optimistic for macro-heavy
		// expressions, so fall back to exact per-candidate checks.
		exact, err := s.groupExact(group, build)
		if err != nil {
			return nil, err
		}
		result = append(result, exact...)
	}

	return result, nil
}

func (s *splitter) groupExact(items []string, build func([]string) string) ([]string, error) {
	var result []string
	var current []string

	for _, item := range items {
		candidate := append(append([]string{}, current...), item)
		expression := build(candidate)
		_, fits, err := s.fit(expression)
		if err != nil {
			return nil, err
		}
		if fits {
			current = candidate
			continue
		}

		if len(current) == 0 {
			return nil, s.partError(expression)
		}

		result = append(result, build(current))
		current = []string{item}

		expression = build(current)
		_, fits, err = s.fit(expression)
		if err != nil {
			return nil, err
		}
		if !fits {
			return nil, s.partError(expression)
		}
	}

	if len(current) > 0 {
		result = append(result, build(current))
	}
	return result, nil
}

func (s *splitter) fit(expression string) (int, bool, error) {
	if utf8.RuneCountInString(expression) > s.sizeLimit {
		return 0, false, nil
	}
	return fit(s.limitEnv, s.countEnv, expression, s.limit)
}

func (s *splitter) count(expression string) (int, error) {
	return countWith(s.countEnv, expression)
}

func (s *splitter) checkPart(expression string) error {
	count, err := s.count(expression)
	if err != nil {
		return err
	}
	if count <= s.limit && utf8.RuneCountInString(expression) <= s.sizeLimit {
		return nil
	}
	return s.partError(expression)
}

func (s *splitter) partError(expression string) error {
	count, err := s.count(expression)
	if err != nil {
		return err
	}
	return fmt.Errorf(
		"cel: part %q has %d nodes and %d code points, limits are %d nodes / %d code points",
		snippet(expression), count, utf8.RuneCountInString(expression), s.limit, s.sizeLimit,
	)
}

func fit(limitEnv, countEnv *celgo.Env, expression string, limit int) (int, bool, error) {
	ast, iss := limitEnv.Parse(expression)
	if iss != nil && iss.Err() != nil {
		if _, err := parseWith(countEnv, expression); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}

	count := celast.NodeCount(ast.NativeRep())
	return count, count <= limit, nil
}

func countWith(env *celgo.Env, expression string) (int, error) {
	ast, err := parseWith(env, expression)
	if err != nil {
		return 0, err
	}
	return celast.NodeCount(ast.NativeRep()), nil
}

func parseWith(env *celgo.Env, expression string) (*celgo.Ast, error) {
	ast, iss := env.Parse(expression)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf(errCelParse, iss.Err())
	}
	return ast, nil
}

func newParseEnv(opts ...celgo.EnvOption) (*celgo.Env, error) {
	envOpts := []celgo.EnvOption{
		celgo.OptionalTypes(),
		celgo.EnableMacroCallTracking(),
		// Splitting must be able to parse expressions that exceed the default
		// parser code point limit; the node limit still bounds the result.
		celgo.ParserExpressionSizeLimit(-1),
	}
	envOpts = append(envOpts, opts...)

	env, err := celgo.NewEnv(envOpts...)
	if err != nil {
		return nil, fmt.Errorf(errCelInit, err)
	}
	return env, nil
}

func flattenBool(expr celast.Expr, op string, operands *[]celast.Expr) {
	if expr.Kind() == celast.CallKind {
		call := expr.AsCall()
		if !call.IsMemberFunction() && call.FunctionName() == op {
			for _, arg := range call.Args() {
				flattenBool(arg, op, operands)
			}
			return
		}
	}
	*operands = append(*operands, expr)
}

func logicalFunction(expr celast.Expr) (string, bool) {
	if expr.Kind() != celast.CallKind {
		return "", false
	}
	call := expr.AsCall()
	if call.IsMemberFunction() {
		return "", false
	}
	op := call.FunctionName()
	if op != opAnd && op != opOr {
		return "", false
	}
	return op, true
}

func expressionText(ast *celgo.Ast, expr celast.Expr) (string, error) {
	text, err := celgo.ExprToString(expr, ast.NativeRep().SourceInfo())
	if err != nil {
		return "", fmt.Errorf("cel: unparse: %v", err)
	}
	return text, nil
}

func quoteFieldName(name string) string {
	if fieldNamePattern.MatchString(name) && name != "in" {
		return name
	}
	return "`" + name + "`"
}

func snippet(text string) string {
	const maxLen = 120
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "..."
}

func kindName(kind celast.ExprKind) string {
	switch kind {
	case celast.CallKind:
		return "call"
	case celast.ComprehensionKind:
		return "comprehension"
	case celast.IdentKind:
		return "identifier"
	case celast.ListKind:
		return "list"
	case celast.LiteralKind:
		return "literal"
	case celast.MapKind:
		return "map"
	case celast.SelectKind:
		return "select"
	case celast.StructKind:
		return "struct"
	default:
		return "unknown"
	}
}
