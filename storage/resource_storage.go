package esstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/clusterpedia-io/clusterpedia/pkg/utils/feature"
	"reflect"
	"strconv"

	"github.com/elastic/go-elasticsearch/v8"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	genericstorage "k8s.io/apiserver/pkg/storage"

	internal "github.com/clusterpedia-io/api/clusterpedia"
	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
)

var (
	supportedOrderByFields = sets.NewString("cluster", "namespace", "name", "created_at", "resource_version")
)

type ResourceStorage struct {
	client *elasticsearch.Client
	codec  runtime.Codec

	storageGroupResource schema.GroupResource
	storageVersion       schema.GroupVersion
	memoryVersion        schema.GroupVersion

	indexName     string
	resourceAlias string

	extractConfig []string

	index *Index
}

func (s *ResourceStorage) GetStorageConfig() *storage.ResourceStorageConfig {
	return &storage.ResourceStorageConfig{
		Codec:                s.codec,
		StorageGroupResource: s.storageGroupResource,
		StorageVersion:       s.storageVersion,
		MemoryVersion:        s.memoryVersion,
	}
}

func (s *ResourceStorage) Create(ctx context.Context, cluster string, obj runtime.Object) error {
	return s.upsert(ctx, cluster, obj)
}

func (s *ResourceStorage) List(ctx context.Context, listObject runtime.Object, opts *internal.ListOptions) error {
	ownerIds, err := s.GetOwnerIds(ctx, opts)
	if err != nil {
		return err
	}
	query, err := s.genListQuery(ownerIds, opts)
	if err != nil {
		return err
	}
	r, err := s.index.Search(ctx, query, []string{s.indexName})
	if err != nil {
		return err
	}
	list, err := meta.ListAccessor(listObject)
	if err != nil {
		return err
	}
	offset, err := strconv.Atoi(opts.Continue)
	if opts.WithContinue != nil && *opts.WithContinue {
		if int64(len(r.GetResources())) == opts.Limit {
			list.SetContinue(strconv.FormatInt(int64(offset)+opts.Limit, 10))
		}
	}

	remain := r.GetTotal() - int64(offset) - int64(len(r.GetResources()))
	list.SetRemainingItemCount(&remain)

	objects := make([]runtime.Object, len(r.GetResources()))
	if unstructuredList, ok := listObject.(*unstructured.UnstructuredList); ok {
		for _, resource := range r.GetResources() {
			object := resource.GetObject()
			uObj := &unstructured.Unstructured{}
			byte, err := json.Marshal(object)
			if err != nil {
				return err
			}
			obj, _, err := s.codec.Decode(byte, nil, uObj)
			if err != nil {
				return err
			}
			if obj != uObj {
				return fmt.Errorf("Failed to decode resource, into is %T", uObj)
			}
			objects = append(objects, uObj)

		}
		for _, object := range objects {
			if err != nil {
				return err
			}
			uObj, ok := object.(*unstructured.Unstructured)
			if !ok {
				return genericstorage.NewInternalError("the converted Object is not *unstructured.Unstructured")
			}
			unstructuredList.Items = append(unstructuredList.Items, *uObj)
		}
		return nil
	}

	listPtr, err := meta.GetItemsPtr(listObject)
	if err != nil {
		return err
	}

	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}

	slice := reflect.MakeSlice(v.Type(), len(objects), len(objects))
	expected := reflect.New(v.Type().Elem()).Interface().(runtime.Object)

	for i, resource := range r.GetResources() {
		object := resource.GetObject()
		byte, err := json.Marshal(object)
		if err != nil {
			return err
		}
		obj, _, err := s.codec.Decode(byte, nil, expected.DeepCopyObject())
		if err != nil {
			return err
		}
		slice.Index(i).Set(reflect.ValueOf(obj).Elem())

	}
	v.Set(slice)
	return nil
}

func (s *ResourceStorage) GetOwnerIds(ctx context.Context, opts *internal.ListOptions) ([]string, error) {
	var empty []string
	switch {
	case len(opts.ClusterNames) != 1:
		return empty, nil
	case opts.OwnerUID != "":
		result, err := s.getUIDs(ctx, opts.ClusterNames[0], []string{opts.OwnerUID}, opts.OwnerSeniority)
		return result, err
	case opts.OwnerName != "":
		result, err := s.getUIDsByName(ctx, opts)
		return result, err

	default:
		return empty, nil
	}
}

func (s *ResourceStorage) getUIDsByName(ctx context.Context, opts *internal.ListOptions) ([]string, error) {
	builder := NewQueryBuilder()
	builder.size = 500
	builder.source = []string{UIDPath}

	if len(opts.Namespaces) != 0 {
		builder.addExpression(NewTerms(NameSpacePath, opts.Namespaces))
	}

	if !opts.OwnerGroupResource.Empty() {
		groupResource := opts.OwnerGroupResource
		builder.addExpression(NewTerms(GroupPath, []string{groupResource.Group}))
		builder.addExpression(NewTerms(ResourcePath, []string{groupResource.Resource}))
	}

	builder.addExpression(NewTerms(NamePath, []string{opts.OwnerName}))

	cluster := opts.ClusterNames[0]
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))

	r, err := s.index.Search(ctx, builder.build(), []string{s.resourceAlias})
	if err != nil {
		return nil, err
	}

	var uids []string
	for _, resource := range r.GetResources() {
		uid := simpleMapExtract("metadata.uid", resource.GetObject())
		if uid == nil {
			return nil, fmt.Errorf("error")
		}
		uids = append(uids, uid.(string))
	}
	return s.getUIDs(ctx, cluster, uids, opts.OwnerSeniority)
}

func (s *ResourceStorage) getUIDs(ctx context.Context, cluster string, uids []string, seniority int) ([]string, error) {
	if seniority == 0 {
		return uids, nil
	}
	builder := NewQueryBuilder()
	builder.size = 500
	builder.source = []string{UIDPath}
	builder.addExpression(NewTerms(OwnerReferencePath, uids))
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))

	r, err := s.index.Search(ctx, builder.build(), []string{s.resourceAlias})
	if err != nil {
		return nil, err
	}
	uids = []string{}
	for _, resource := range r.GetResources() {
		result := simpleMapExtract("metadata.uid", resource.GetObject())
		if result == nil {
			return nil, fmt.Errorf("extract uid failure, targetObject is %v", resource.GetObject())
		}
		uid, ok := result.(string)
		if !ok {
			return nil, fmt.Errorf("result to string failure, targetObject is %v", resource.GetObject())
		}
		uids = append(uids, uid)
	}
	return s.getUIDs(ctx, cluster, uids, seniority-1)
}

func (s *ResourceStorage) Get(ctx context.Context, cluster, namespace, name string, into runtime.Object) error {
	builder := NewQueryBuilder()
	builder.addExpression(NewTerms(GroupPath, []string{s.storageGroupResource.Group}))
	builder.addExpression(NewTerms(VersionPath, []string{s.storageVersion.Version}))
	builder.addExpression(NewTerms(ResourcePath, []string{s.storageGroupResource.Resource}))
	builder.addExpression(NewTerms(NamePath, []string{name}))
	builder.addExpression(NewTerms(NameSpacePath, []string{namespace}))
	builder.addExpression(NewTerms(ClusterPath, []string{cluster}))

	r, err := s.index.Search(ctx, builder.build(), []string{s.indexName})
	if err != nil {
		return err
	}
	cnt := len(r.GetResources())
	if cnt == 0 {
		return genericstorage.NewKeyNotFoundError(fmt.Sprintf("%s/%s", cluster, namespace+"/"+name), 0)
	}
	resource := r.GetResources()[0]
	object := resource.Object
	byte, err := json.Marshal(object)
	if err != nil {
		return err
	}
	obj, _, err := s.codec.Decode(byte, nil, into)
	if err != nil {
		return err
	}
	if obj != into {
		return fmt.Errorf("failed to decode resource, into is %v", into)
	}
	return nil
}

func (s *ResourceStorage) Delete(ctx context.Context, cluster string, obj runtime.Object) error {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	//dirty data will cause errors
	if len(metaobj.GetUID()) == 0 {
		return nil
	}
	err = s.index.DeleteById(ctx, string(metaobj.GetUID()), s.indexName)
	if err != nil {
		return err
	}
	return nil
}

func (s *ResourceStorage) Update(ctx context.Context, cluster string, obj runtime.Object) error {
	return s.upsert(ctx, cluster, obj)
}

func (s *ResourceStorage) upsert(ctx context.Context, cluster string, obj runtime.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" {
		return fmt.Errorf("%s: kind is required", gvk)
	}
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	custom := make(map[string]string)
	if unstructured, ok := obj.(*unstructured.Unstructured); ok && len(s.extractConfig) > 0 {
		for _, path := range s.extractConfig {
			result := simpleMapExtract(path, unstructured.Object)
			if result != nil {
				value, err := json.Marshal(result)
				if err == nil {
					custom[path] = string(value)
				}
			}
		}
	}

	if feature.FeatureGate.Enabled(AllowObjectFullTextSearch) {
		value, err := json.Marshal(obj)
		if err == nil {
			custom["fullTextObject"] = string(value)
		}
	}

	resource := s.genDocument(metaObj, gvk, custom)
	err = s.index.Upsert(ctx, s.indexName, string(metaObj.GetUID()), resource)
	if err != nil {
		return err
	}
	return nil
}

func (s *ResourceStorage) genDocument(metaObj metav1.Object, gvk schema.GroupVersionKind, custom map[string]string) map[string]interface{} {
	requestBody := map[string]interface{}{
		"group":           s.storageGroupResource.Group,
		"version":         s.storageVersion.Version,
		"resource":        s.storageGroupResource.Resource,
		"kind":            gvk.Kind,
		"name":            metaObj.GetName(),
		"namespace":       metaObj.GetNamespace(),
		"resourceVersion": metaObj.GetResourceVersion(),
		"object":          metaObj,
	}
	if len(custom) > 0 {
		requestBody["custom"] = custom
	}
	return requestBody
}

func (s *ResourceStorage) Watch(_ context.Context, _ *internal.ListOptions) (watch.Interface, error) {
	return nil, apierrors.NewMethodNotSupported(s.storageGroupResource, "watch")
}
