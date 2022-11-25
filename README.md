# Elasticsearch Storage
The elasticsearch storage plugin enables clusterpedia to store and search data in Elasticsearch
## Build and Run
`git clone` repo
```bash
$ git clone --recursive https://github.com/clusterpedia-io/elasticsearch-storage.git
$ cd es-storage-layer
```

build storage layer plugin
```bash
$ make build-plugin

$ # check plugin
$ file ./plugins/elasticsearch-storage-layer.so
./plugins/sample-storage-layer.so: Mach-O 64-bit dynamically linked shared library x86_64
```

build clusterpedia components for the debug
```bash
$ make build-components
$ ls -al ./bin
drwxr-xr-x   6 icebergu  staff       192 11  7 11:17 .
drwxr-xr-x  16 icebergu  staff       512 11  7 11:15 ..
-rwxr-xr-x   1 icebergu  staff  90707488 11  7 11:15 apiserver
-rwxr-xr-x   1 icebergu  staff  91896016 11  7 11:16 binding-apiserver
-rwxr-xr-x   1 icebergu  staff  82769728 11  7 11:16 clustersynchro-manager
-rwxr-xr-x   1 icebergu  staff  45682000 11  7 11:17 controller-manager
```

run clusterpedia apiserver
```bash
$ STORAGE_PLUGINS=./plugins ./bin/apiserver --storage-name=elasticsearch --storage-config=./config.yaml <... other flags>
```

run clusterpedia clustersynchro-manager
```bash
$ STORAGE_PLUGINS=./plugins ./bin/clustersynchro-manager --storage-name=elasticsearch --storage-config=./config.yaml <... other flags>
```