package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

const indexPrefix = "clusterpedia"

type StorageFactory struct {
	index      *Index
	indexAlias string
}

func (s *StorageFactory) NewResourceStorage(config *storage.ResourceStorageConfig) (storage.ResourceStorage, error) {
	storage := &ResourceStorage{
		codec: config.Codec,

		storageGroupResource: config.StorageGroupResource,
		storageVersion:       config.StorageVersion,
		memoryVersion:        config.MemoryVersion,
		resourceAlias:        s.indexAlias,
		index:                s.index,
	}
	// indexAlias: ${prefix}-${group}-${resource}
	storage.indexName = generateIndexName(config.StorageGroupResource.Group, config.StorageGroupResource.Resource)
	var mapping = GetIndexMapping(s.indexAlias, config.GroupResource)
	err := ensureIndex(s.index.client, mapping, storage.indexName)
	if err != nil {
		return nil, err
	}
	if storage.storageGroupResource.Resource == ResourceConfigmap {
		storage.extractConfig = []string{"data"}
	}
	return storage, nil
}

func generateIndexName(group, resource string) string {
	return fmt.Sprintf("%s-%s-%s", indexPrefix, group, resource)
}

func (s *StorageFactory) NewCollectionResourceStorage(cr *internal.CollectionResource) (storage.CollectionResourceStorage, error) {
	return NewCollectionResourceStorage(s.index.client, s.indexAlias, cr), nil
}

func (s *StorageFactory) GetResourceVersions(ctx context.Context, cluster string) (map[schema.GroupVersionResource]map[string]interface{}, error) {
	resourceVersions := make(map[schema.GroupVersionResource]map[string]interface{})
	builder := NewQueryBuilder()
	builder.source = []string{"group", "version", "resource", "namespace", "name", "resourceVersion"}
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))
	resps, err := s.index.SearchAll(ctx, builder.build(), []string{s.indexAlias})
	if err != nil {
		esError, ok := err.(*ESError)
		if ok && esError.StatusCode == 404 {
			return resourceVersions, nil
		}
		return resourceVersions, err
	}
	for _, r := range resps {
		for _, item := range r.Hits.Hits {
			resource := item.Source
			gvr := resource.GroupVersionResource()
			versions := resourceVersions[gvr]
			if versions == nil {
				versions = make(map[string]interface{})
				resourceVersions[gvr] = versions
			}
			key := resource.GetName()
			if resource.GetNamespace() != "" {
				key = resource.GetNamespace() + "/" + resource.GetName()
			}
			versions[key] = resource.GetResourceVersion()
		}
	}
	return resourceVersions, nil
}

func (s *StorageFactory) CleanCluster(ctx context.Context, cluster string) error {
	var buf bytes.Buffer
	builder := NewQueryBuilder()
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))
	query := builder.build()
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	indexNames, err := s.index.ListIndex()
	if err != nil {
		return err
	}
	var targetIndex []string
	for _, indexName := range indexNames {
		if strings.HasPrefix(indexName, indexPrefix) {
			targetIndex = append(targetIndex, indexName)
		}
	}
	s.index.DeleteByQuery(ctx, query, targetIndex...)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageFactory) CleanClusterResource(ctx context.Context, cluster string, gvr schema.GroupVersionResource) error {
	builder := NewQueryBuilder()
	builder.addExpression(NewTerms(GroupPath, []string{gvr.Group}))
	builder.addExpression(NewTerms(VersionPath, []string{gvr.Version}))
	builder.addExpression(NewTerms(ResourcePath, []string{gvr.Resource}))
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))
	indexName := generateIndexName(gvr.Group, gvr.Resource)
	err := s.index.DeleteByQuery(ctx, builder.build(), indexName)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageFactory) GetCollectionResources(ctx context.Context) ([]*internal.CollectionResource, error) {
	var crs []*internal.CollectionResource
	for _, cr := range collectionResources {
		crs = append(crs, cr.DeepCopy())
	}
	return crs, nil
}

func (s *StorageFactory) PrepareCluster(cluster string) error {
	return nil
}

func (s *StorageFactory) GetSupportedRequestVerbs() []string {
	return []string{"get", "list"}
}
