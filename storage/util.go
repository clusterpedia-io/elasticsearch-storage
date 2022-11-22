package esstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage/internalstorage"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func (s *ResourceStorage) genListQuery(ownerIds []string, opts *internal.ListOptions) (map[string]interface{}, error) {
	builder := &QueryBuilder{}
	if opts.ClusterNames != nil {
		queryItem := NewTerms("object.metadata.annotations.shadow.clusterpedia.io/cluster-name", opts.ClusterNames)
		builder.addExpression(queryItem)
	}
	if opts.Namespaces != nil {
		queryItem := NewTerms("namespaces", opts.Namespaces)
		builder.addExpression(queryItem)
	}
	if opts.Names != nil {
		queryItem := NewTerms("name", opts.Names)
		builder.addExpression(queryItem)
	}

	if opts.Since != nil || opts.Before != nil {
		queryItem := &RangeExpression{}
		queryItem = NewRange("object.metadata.creationTimestamp", opts.Since, opts.Before)
		builder.addExpression(queryItem)
	}

	if opts.LabelSelector != nil {
		if requirements, selectable := opts.LabelSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				values := requirement.Values().List()
				queryItem := NewTerms("object.metadata.labels", values)
				switch requirement.Operator() {
				case selection.Exists, selection.DoesNotExist, selection.Equals, selection.DoubleEquals:
					builder.addExpression(queryItem)
				case selection.NotEquals, selection.NotIn:
					queryItem.SetLogicType(MustNot)
					builder.addExpression(queryItem)
				default:
					continue
				}
			}
		}
	}

	if opts.ExtraLabelSelector != nil {
		if requirements, selectable := opts.ExtraLabelSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				switch requirement.Key() {
				case internalstorage.SearchLabelFuzzyName:
					for _, name := range requirement.Values().List() {
						name = strings.TrimSpace(name)
						values := []string{name}
						queryItem := NewFuzzy("name", values)
						builder.addExpression(queryItem)
					}
				}
			}
		}
	}

	if opts.EnhancedFieldSelector != nil {
		if requirements, selectable := opts.EnhancedFieldSelector.Requirements(); selectable {
			for _, requirement := range requirements {
				var (
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
					queryItem := NewTerms(path, values)
					builder.addExpression(queryItem)
				case selection.NotEquals, selection.NotIn:
					queryItem := NewTerms(path, values)
					queryItem.SetLogicType(MustNot)
					builder.addExpression(queryItem)
				default:
					return nil, nil
				}

			}
		}
	}

	if len(opts.ClusterNames) == 1 && (len(opts.OwnerUID) != 0 || len(opts.OwnerName) != 0) {
		queryItem := NewTerms("object.metadata.ownerReferences.uid", ownerIds)
		builder.addExpression(queryItem)
	}

	groupItem := NewTerms("group", []string{s.storageVersion.Group})
	builder.addExpression(groupItem)
	versionItem := NewTerms("version", []string{s.storageVersion.Version})
	builder.addExpression(versionItem)
	resourceItem := NewTerms("resource", []string{s.storageGroupResource.Resource})
	builder.addExpression(resourceItem)

	size := 500
	if opts.Limit != -1 {
		size = int(opts.Limit)
	}
	offset, _ := strconv.Atoi(opts.Continue)

	for _, orderby := range opts.OrderBy {
		field := orderby.Field
		if supportedOrderByFields.Has(field) {
			//TODO add sort
		}
	}
	builder.size = size
	builder.from = offset

	return builder.builder(), nil
}

func EnsureIndex(client *elasticsearch.Client, mapping string, indexName string) error {
	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}
	resp, err := req.Do(context.Background(), client)
	if err != nil {
		klog.Errorf("Error getting response: %v", err)
		return err
	}
	if resp.IsError() {
		msg := resp.String()
		if strings.Contains(resp.String(), "resource_already_exists_exception") {
			klog.Warningf("index %s already exists", indexName)
			return nil
		}
		return fmt.Errorf(msg)
	}
	return nil
}

func SimpleMapExtract(path string, object map[string]interface{}) interface{} {
	fields := strings.Split(path, ".")
	var cur interface{}
	cur = object
	for i := range fields {
		field := fields[i]
		mapObj, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		cur = mapObj[field]
	}
	return cur
}
