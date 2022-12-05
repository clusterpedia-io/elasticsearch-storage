package main

import (
	plugin "github.com/clusterpedia-io/elasticsearch-storage/storage"
)

func init() {
	plugin.RegisterStorageLayer()
}
