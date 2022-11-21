package esstorage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	ResourceConfigmap = "configmaps"
	ResourceSecret    = "secrets"
	ResourceEvent     = "events"
)

var mappingTemplate = `{
  "aliases": {
	"%s": {}
  },
  "settings": {
    "index": {
      "number_of_shards": 1,
      "auto_expand_replicas": "0-1",
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "_source":{
		"excludes":["custom"]
    },
    "properties": {
      "group": {
        "type": "keyword"
      },
      "version": {
        "type": "keyword"
      },
      "resource": {
        "type": "keyword"
      },
      "name": {
        "type": "keyword"
      },
      "namespace": {
        "type": "keyword"
      },
      "resource_version": {
        "type": "keyword"
      },
      "object": {
        "properties": {
          "metadata": {
            "properties": {
              "annotations": {
                "type": "flattened"
              },
              "managedFields": {
                "type": "object",
                "enabled":false	
              },
              "creationTimestamp": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
              },
              "deletionTimestamp": {
                "type": "date", 
                "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
              },
              "labels": {
                "type": "flattened"
              },
              "name": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "namespace": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "ownerReferences": {
                "type": "flattened"
              },
              "resourceVersion": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          %s
        }
      }
    }
  }
}`

var common = `
    "spec":{
        "type":"flattened",
        "ignore_above": 256
    }	
`
var configmap = `
    "data": {
        "type": "object",
	    "enabled":false
    },
    "binaryData": {
        "type": "object",
		"enabled":false
    }
`
var secret = `
    "data": {
        "type": "object",
        "enabled":false
    },
    "stringData": {
        "type": "object",
        "enabled":false
    }
`
var event = `
    "involvedObject": {
        "type": "flattened",
    },
    "source": {
        "type": "flattened",
    },
    "firstTimestamp": {
        "type": "date", 
		"format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
    },
    "lastTimestamp": {
        "type": "date", 
		"format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
    },
    "eventTime": {
        "type": "date", 
		"format": "yyyy-MM-dd'T'HH:mm:ss'Z'"
    },
    "related": {
        "type": "flattened",
    },
    "series": {
        "type": "flattened",
    }
`

func GetIndexMapping(alias string, storageGroupResource schema.GroupResource) string {
	resource := storageGroupResource.Resource
	switch resource {
	case ResourceConfigmap:
		return fmt.Sprintf(mappingTemplate, alias, configmap)
	case ResourceSecret:
		return fmt.Sprintf(mappingTemplate, alias, secret)
	case ResourceEvent:
		return fmt.Sprintf(mappingTemplate, alias, event)
	default:
		return fmt.Sprintf(mappingTemplate, alias, common)
	}
}
