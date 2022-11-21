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
	err := EnsureIndex(s.index.client, mapping, storage.indexName)
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
	query := map[string]interface{}{
		"_source": []string{"group", "version", "resource", "namespace", "name", "resourceVersion"},
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
			},
		},
	}
	resps, err := s.index.SearchAll(ctx, query, []string{s.indexAlias})

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
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
			},
		},
	}
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
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"group": gvr.Group,
						},
					},
					{
						"match": map[string]interface{}{
							"version": gvr.Version,
						},
					},
					{
						"match": map[string]interface{}{
							"resource": gvr.Resource,
						},
					},
					{
						"match": map[string]interface{}{
							"object.metadata.annotations.shadow.clusterpedia.io/cluster-name": cluster,
						},
					},
				},
			},
		},
	}
	indexName := generateIndexName(gvr.Group, gvr.Resource)
	err := s.index.DeleteByQuery(ctx, query, indexName)
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
