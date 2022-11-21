package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sjson "k8s.io/apimachinery/pkg/util/json"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

type CollectionResourceStorage struct {
	client    *elasticsearch.Client
	indexName string

	collectionResource *internal.CollectionResource
}

func NewCollectionResourceStorage(client *elasticsearch.Client, indexName string, cr *internal.CollectionResource) storage.CollectionResourceStorage {
	return &CollectionResourceStorage{
		client:             client,
		indexName:          indexName,
		collectionResource: cr.DeepCopy(),
	}
}

func (s *CollectionResourceStorage) Get(ctx context.Context, opts *internal.ListOptions) (*internal.CollectionResource, error) {
	var boolShouldConditionArrays []interface{}

	for _, rt := range s.collectionResource.ResourceTypes {
		var boolMustConditionArrays []interface{}
		boolMustConditionArrays = append(boolMustConditionArrays, map[string]interface{}{
			"match": map[string]interface{}{"group": rt.Group},
		})
		if rt.Resource != "" {
			boolMustConditionArrays = append(boolMustConditionArrays, map[string]interface{}{
				"match": map[string]interface{}{"resource": rt.Resource},
			})
		}
		if rt.Version != "" {
			boolMustConditionArrays = append(boolMustConditionArrays, map[string]interface{}{
				"match": map[string]interface{}{"version": rt.Version},
			})
		}
		mustCondition := map[string]interface{}{
			"bool": map[string]interface{}{"must": boolMustConditionArrays},
		}

		boolShouldConditionArrays = append(boolShouldConditionArrays, mustCondition)
	}

	//if opts.Limit != -1 {
	//	queryCondition["size"] = int(opts.Limit)
	//}
	//
	//offset, err := strconv.Atoi(opts.Continue)
	//if err == nil {
	//	queryCondition["from"] = offset
	//}

	queryCondition := map[string]interface{}{"should": boolShouldConditionArrays}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": queryCondition,
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf(res.String())
	}
	defer res.Body.Close()
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}

	objects := make([]runtime.Object, len(r.GetResources()))

	collection := &internal.CollectionResource{
		TypeMeta:   s.collectionResource.TypeMeta,
		ObjectMeta: s.collectionResource.ObjectMeta,
		Items:      make([]runtime.Object, 0, len(r.GetResources())),
	}

	for _, item := range r.GetResources() {
		object := item.Object

		byte, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		unObj, err := convertToUnstructured(byte)
		if err != nil {
			return nil, err
		}
		objects = append(objects, unObj)

		gvrs := make(map[schema.GroupVersionResource]struct{})
		if resourceType := item.GetResourceType(); !resourceType.Empty() {
			gvr := resourceType.GroupVersionResource()
			if _, ok := gvrs[gvr]; !ok {
				gvrs[gvr] = struct{}{}
				collection.ResourceTypes = append(collection.ResourceTypes, internal.CollectionResourceType{
					Group:    resourceType.Group,
					Resource: resourceType.Resource,
					Version:  resourceType.Version,
					Kind:     resourceType.Kind,
				})
			}
		}
	}
	collection.Items = objects

	//if opts.WithContinue != nil && *opts.WithContinue {
	//	if int64(len(objects)) == opts.Limit {
	//		collection.Continue = strconv.FormatInt(int64(offset)+opts.Limit, 10)
	//	}
	//}

	//todo
	//if amount != nil {
	//	// When offset is too large, the data in the response is empty and the remaining count is negative.
	//	// This ensures that `amount = offset + len(objects) + remain`
	//	remain := *amount - offset - int64(len(items))
	//	collection.RemainingItemCount = &remain
	//}

	return collection, nil
}

func convertToUnstructured(data []byte) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if err := k8sjson.Unmarshal(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
