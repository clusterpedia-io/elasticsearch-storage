package esstorage

import (
	clusterpediafeature "github.com/clusterpedia-io/clusterpedia/pkg/utils/feature"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

const (
	// AllowObjectFullTextSearch is a feature gate for the clustersynchro-manager Indexing k8s objects as string.
	//
	// owner: @hanweisen
	// alpha: v0.1.0
	AllowObjectFullTextSearch featuregate.Feature = "AllowObjectFullTextSearch"
)

func init() {
	runtime.Must(clusterpediafeature.MutableFeatureGate.Add(defaultElasticSearchStorageFeatureGates))
}

// AllowObjectFullTextSearch consists of all known custom esstorage feature keys.
// To add a new feature, define a key for it above and add it here.
var defaultElasticSearchStorageFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	AllowObjectFullTextSearch: {Default: true, PreRelease: featuregate.Alpha},
}
