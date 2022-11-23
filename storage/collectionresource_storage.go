package esstorage

import (
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v8"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sjson "k8s.io/apimachinery/pkg/util/json"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

type CollectionResourceStorage struct {
	index     *Index
	indexName string

	collectionResource *internal.CollectionResource
}

func NewCollectionResourceStorage(client *elasticsearch.Client, indexName string, cr *internal.CollectionResource) storage.CollectionResourceStorage {
	return &CollectionResourceStorage{
		index:              NewIndex(client),
		indexName:          indexName,
		collectionResource: cr.DeepCopy(),
	}
}

func (s *CollectionResourceStorage) Get(ctx context.Context, opts *internal.ListOptions) (*internal.CollectionResource, error) {
	builder := NewQueryBuilder()
	for _, rt := range s.collectionResource.ResourceTypes {
		groupTerm := NewTerms(GroupPath, []string{rt.Group})
		groupTerm.SetLogicType(Should)
		builder.addExpression(groupTerm)

		if len(rt.Resource) > 0 {
			resourceTerm := NewTerms(ResourcePath, []string{rt.Resource})
			resourceTerm.SetLogicType(Should)
			builder.addExpression(resourceTerm)
		}

		if len(rt.Version) > 0 {
			versionTerm := NewTerms(VersionPath, []string{rt.Version})
			versionTerm.SetLogicType(Should)
			builder.addExpression(versionTerm)
		}
	}

	err := applyListOptionToQueryBuilder(builder, opts)
	if err != nil {
		return nil, err
	}

	r, err := s.index.Search(ctx, builder.build(), []string{s.indexName})
	if err != nil {
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
	return collection, nil
}

func convertToUnstructured(data []byte) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if err := k8sjson.Unmarshal(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
