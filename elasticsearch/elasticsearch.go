package elasticsearch_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/update"
	"github.com/elastic/go-elasticsearch/v9/typedapi/esdsl"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
)

var valueTypeMapping = map[string]func() types.PropertyVariant{
	"integer": func() types.PropertyVariant { return esdsl.NewIntegerNumberProperty() },
	"boolean": func() types.PropertyVariant { return esdsl.NewBooleanProperty() },
	"date":    func() types.PropertyVariant { return esdsl.NewDateProperty() },
	"float":   func() types.PropertyVariant { return esdsl.NewFloatNumberProperty() },
	"byte":    func() types.PropertyVariant { return esdsl.NewByteNumberProperty() },
}

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

	elasticSearchClient struct {
		client elasticsearch.TypedClient
	}
)

func (e *elasticSearchClient) CreateIndex(ctx context.Context, indexName string, schema map[string]string) (err error) {
	typeMapping := esdsl.NewTypeMapping()
	for field, fieldType := range schema {
		newProp, ok := valueTypeMapping[fieldType]
		if !ok {
			return fmt.Errorf("unsupported field type %q for field %q", fieldType, field)
		}
		typeMapping.AddProperty(field, newProp())
	}

	_, err = e.client.Indices.Create(indexName).Mappings(typeMapping).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *elasticSearchClient) DeleteIndex(ctx context.Context, indexName string) (err error) {
	_, err = e.client.Indices.Delete(indexName).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *elasticSearchClient) IndexExists(ctx context.Context, indexName string) (bool, error) {
	exist, err := e.client.Indices.Exists(indexName).IsSuccess(ctx)
	if err != nil {
		return false, err
	}
	return exist, nil
}

func (e *elasticSearchClient) InsertDocument(ctx context.Context, indexName string, document interface{}) (isSuccess bool, err error) {
	documentByte, err := json.Marshal(document)
	if err != nil {
		return 1 == 0, err
	}
	response, err := e.client.Index(indexName).Raw(bytes.NewReader(documentByte)).Do(ctx)
	if err != nil {
		return 1 == 0, err
	}
	return response != nil, nil
}

func (e *elasticSearchClient) GetDocument(ctx context.Context, indexName, id string) (json.RawMessage, error) {
	res, err := e.client.Get(indexName, id).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return nil, nil
	}
	return nil, nil
}

func (e *elasticSearchClient) UpdateDocument(ctx context.Context, indexName, id string, document interface{}) (bool, error) {
	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return false, err
	}
	_, err = e.client.Update(indexName, id).Request(
		&update.Request{
			Doc: json.RawMessage(jsonDoc),
		},
	).Do(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *elasticSearchClient) DeleteDocument(ctx context.Context, indexName, id string) error {
	_, err := e.client.Delete(indexName, id).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *elasticSearchClient) BulkInsert(ctx context.Context, indexName string, documents []interface{}) error {
	for _, document := range documents {
		_, err := e.InsertDocument(ctx, indexName, document)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *elasticSearchClient) SearchDocument(ctx context.Context, indexName string, request *search.Request) (document interface{}, err error) {
	resp, err := e.client.Search().Index(indexName).Request(request).Do(ctx)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *elasticSearchClient) Count(ctx context.Context, indexName string, request *search.Request) (int64, error) {
	return 0, nil
}
