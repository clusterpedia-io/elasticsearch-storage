package esstorage

import (
	"github.com/jinzhu/configor"
	"k8s.io/klog/v2"

	"github.com/clusterpedia-io/clusterpedia/pkg/storage"
	"github.com/elastic/go-elasticsearch/v8"
)

const (
	StorageName = "es-storage-layer"
)

func RegisterStorageLayer() {
	storage.RegisterStorageFactoryFunc(StorageName, NewStorageFactory)
	klog.Infof("Successful register storage :%s", StorageName)
}

func NewStorageFactory(configPath string) (storage.StorageFactory, error) {
	cfg := &Config{}
	if err := configor.Load(cfg, configPath); err != nil {
		return nil, err
	}

	return &StorageFactory{
		indexAlias: "clusterpedia-resource",
		index:      NewIndex(initESClient(cfg)),
	}, nil
}

func initESClient(cfg *Config) *elasticsearch.Client {
	es, err := elasticsearch.NewClient(*cfg.genESCfg())
	if err != nil {
		klog.Fatalf("Error: NewClient(): %s", err)
	}
	return es
}

func (c *Config) genESCfg() *elasticsearch.Config {
	cfg := &elasticsearch.Config{
		Addresses: c.Addresses,
	}
	if len(c.UserName) > 0 {
		cfg.Username = c.UserName
		cfg.Password = c.Password
	}
	return cfg
}
