package elasticsearch_client

import (
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
)

type (
	ElasticSearchClient interface {
		CreateIndex(ctx context.Context, indexName string, schema map[string]string) (err error)
		DeleteIndex(ctx context.Context, indexName string) (err error)
		IndexExists(ctx context.Context, indexName string) (bool, error)

		InsertDocument(ctx context.Context, indexName string, document interface{}) (isSuccess bool, err error)
		GetDocument(ctx context.Context, indexName, id string) (json.RawMessage, error)
		UpdateDocument(ctx context.Context, indexName, id string, document interface{}) (bool, error)
		DeleteDocument(ctx context.Context, indexName, id string) error
		BulkInsert(ctx context.Context, indexName string, documents []interface{}) error

		SearchDocument(ctx context.Context, indexName string, request *search.Request) (document interface{}, err error)
		Count(ctx context.Context, indexName string, request *search.Request) (int64, error)
	}
)
