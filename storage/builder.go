package esstorage

import (
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation/field"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/api/clusterpedia/fields"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage/internalstorage"
)

const (
	ClusterNamePath = "object.metadata.annotations.shadow.clusterpedia.io/cluster-name"
	LabelsPath      = "object.metadata.labels"
	GroupPath       = "group"
	VersionPath     = "version"
	ResourcePath    = "resource"
	NamespacesPath  = "namespace"
	NamePath        = "name"
	FuzzyPath       = "fuzzy"
	RangePath       = "range"
	TermsPath       = "terms"
	TermPath        = "term"
	UIDPath         = "object.metadata.ownerReferences.uid"
	TimestampPath   = "object.metadata.creationTimestamp"
	GTEPath         = "gte"
	LTEPath         = "lte"
)

type ESQueryExpression struct {
	key    string
	values interface{}

	notFlag, fuzzyFlag, rangeFlag bool `default:"false"`
	query                         map[string]interface{}
}

func newESQueryExpression(path string, values interface{}) *ESQueryExpression {
	return &ESQueryExpression{key: path, values: values}
}

func newESQueryExpressionList() []*ESQueryExpression {
	return []*ESQueryExpression{}
}

func labelQuery(requirement labels.Requirement, extralFlag bool) *ESQueryExpression {
	var esQuery *ESQueryExpression
	if extralFlag {
		switch requirement.Key() {
		case internalstorage.SearchLabelFuzzyName:
			for _, name := range requirement.Values().List() {
				name = strings.TrimSpace(name)
				esQuery = newESQueryExpression(NamePath, name)
				esQuery.fuzzyFlag = true
			}
		}
		return esQuery
	} else {
		values := requirement.Values().List()
		switch requirement.Operator() {
		case selection.Exists, selection.DoesNotExist, selection.Equals, selection.DoubleEquals:
			esQuery = newESQueryExpression(LabelsPath, values)
		case selection.NotEquals, selection.NotIn:
			esQuery = newESQueryExpression(LabelsPath, values)
			esQuery.notFlag = true
		default:
			return nil
		}
		return esQuery
	}
}

func fieldQuery(requirement fields.Requirement) (*ESQueryExpression, error) {
	var (
		esQuery     *ESQueryExpression
		fields      []string
		fieldErrors field.ErrorList
	)
	for _, f := range requirement.Fields() {
		if f.IsList() {
			fieldErrors = append(fieldErrors, field.Invalid(f.Path(), f.Name(), fmt.Sprintf("Storage<%s>: Not Support list field", StorageName)))
			continue
		}
		fields = append(fields, f.Name())
	}
	if len(fieldErrors) != 0 {
		return nil, apierrors.NewInvalid(schema.GroupKind{Group: internal.GroupName, Kind: "ListOptions"}, "fieldSelector", fieldErrors)
	}
	fields = append(fields, "")
	copy(fields[1:], fields[0:])
	fields[0] = "object"
	path := strings.Join(fields, ".")
	values := requirement.Values().List()
	switch requirement.Operator() {
	case selection.Exists, selection.DoesNotExist, selection.Equals, selection.DoubleEquals:
		esQuery = newESQueryExpression(path, values)
	case selection.NotEquals, selection.NotIn:
		esQuery = newESQueryExpression(path, values)
		esQuery.notFlag = true
	default:
		return nil, nil
	}
	return esQuery, nil
}

func Build(esQueryItems []*ESQueryExpression, size, offset int) map[string]interface{} {
	var mustFilter, mustNotFilter []map[string]interface{}
	for i := range esQueryItems {
		if esQueryItems[i].fuzzyFlag {
			esQueryItems[i].query = map[string]interface{}{
				FuzzyPath: map[string]interface{}{
					esQueryItems[i].key: esQueryItems[i].values,
				},
			}
		} else if esQueryItems[i].rangeFlag {
			esQueryItems[i].query = map[string]interface{}{
				RangePath: map[string]interface{}{
					TimestampPath: map[string]interface{}{
						GTEPath: esQueryItems[i].values,
					},
				},
			}
		} else {
			if _, ok := esQueryItems[i].values.([]string); ok {
				esQueryItems[i].query = map[string]interface{}{
					TermsPath: map[string]interface{}{
						esQueryItems[i].key: esQueryItems[i].values,
					},
				}
			} else {
				esQueryItems[i].query = map[string]interface{}{
					TermPath: map[string]interface{}{
						esQueryItems[i].key: esQueryItems[i].values,
					},
				}
			}
		}
		if esQueryItems[i].notFlag {
			mustNotFilter = append(mustNotFilter, esQueryItems[i].query)
		} else {
			mustFilter = append(mustFilter, esQueryItems[i].query)
		}
	}

	query := map[string]interface{}{
		"size": size,
		"from": offset,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must":     mustFilter,
				"must_not": mustNotFilter,
			},
		},
	}
	return query
}
