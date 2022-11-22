package esstorage

import v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type LogicType int

const (
	Must = iota
	MustNot
	Should
	ShouldNot
)

type Expression interface {
	ToMap() map[string]interface{}
	LogicType() LogicType
}

type Basic struct {
	logicType LogicType
}

func (b *Basic) LogicType() LogicType {
	return b.logicType
}

func (b *Basic) SetLogicType(t LogicType) {
	b.logicType = t
}

type QueryBuilder struct {
	size        int
	from        int
	source      []string
	expressions []Expression
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		size: -1,
		from: -1,
	}
}

func (q *QueryBuilder) addExpression(exp Expression) {
	q.expressions = append(q.expressions, exp)
}

func (q *QueryBuilder) build() map[string]interface{} {
	var mustFilter, mustNotFilter []map[string]interface{}
	for i := range q.expressions {
		if q.expressions[i].LogicType() == Must {
			mustFilter = append(mustFilter, q.expressions[i].ToMap())
		} else {
			mustNotFilter = append(mustNotFilter, q.expressions[i].ToMap())
		}
	}

	bool := map[string]interface{}{}
	if len(mustFilter) > 0 {
		bool["must"] = mustFilter
	}
	if len(mustNotFilter) > 0 {
		bool["must_not"] = mustNotFilter
	}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": bool,
		},
	}
	if q.size >= 0 {
		query["size"] = q.size
	}
	if q.from >= 0 {
		query["from"] = q.from
	}
	if len(q.source) > 0 {
		query["_source"] = q.source
	}

	return query
}

type TermsExpression struct {
	Basic
	path  string
	value []string
}

func NewTerms(path string, value []string) *TermsExpression {
	return &TermsExpression{
		path:  path,
		value: value,
	}
}

func (t *TermsExpression) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"terms": map[string]interface{}{
			t.path: t.value,
		},
	}
}

type FuzzyExpression struct {
	Basic
	path  string
	value []string
}

func NewFuzzy(path string, value []string) *FuzzyExpression {
	return &FuzzyExpression{
		path:  path,
		value: value,
	}
}

func (t *FuzzyExpression) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"fuzzy": map[string]interface{}{
			t.path: t.value,
		},
	}
}

type RangeExpression struct {
	Basic
	path string
	gte  *v1.Time
	lte  *v1.Time
}

func NewRange(path string, gte, lte *v1.Time) *RangeExpression {
	return &RangeExpression{
		path: path,
		gte:  gte,
		lte:  lte,
	}
}

func (t *RangeExpression) ToMap() map[string]interface{} {
	value := map[string]interface{}{}
	if t.gte != nil {
		value["gte"] = t.gte.Unix()
	}
	if t.lte != nil {
		value["lte"] = t.lte.Unix()
	}
	return map[string]interface{}{
		"range": map[string]interface{}{
			t.path: value,
		},
	}
}

type ExistExpression struct {
	Basic
	path string
}

func (t *ExistExpression) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"exist": map[string]interface{}{
			t.path: "",
		},
	}
}
