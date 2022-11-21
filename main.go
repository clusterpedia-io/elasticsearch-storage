package main

import (
	plugin "github.com/clusterpedia-io/es-storage-layer/storage"
)

func init() {
	plugin.RegisterStorageLayer()
}
