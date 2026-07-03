package tigris

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/tigrisdata/storage-go"
)

const (
	tigrisBaseURL = `https://%s.fly.storage.tigris.dev/%s`
)

func NewTigrisClient(config StorageManagerConfig) (*tigrisClient, error) {
	ctx := context.Background()
	tigrisStorageClient, err := storage.New(ctx,
		storage.WithEndpoint(config.EndpointURL),
		storage.WithAccessKeypair(config.AccessKeyID, config.SecretAccessKey),
		storage.WithRegion(config.Region),
	)
	if err != nil {
		return nil, err
	}

	return &tigrisClient{
		storageManager:      config,
		tigrisStorageClient: tigrisStorageClient,
	}, nil
}

var _ StorageManager = &tigrisClient{}

// CreateBucket implements [StorageManager].
func (t *tigrisClient) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := t.tigrisStorageClient.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}
	return nil
}

// CreateForkBucket implements [StorageManager].
func (t *tigrisClient) CreateForkBucket(ctx context.Context, bucketName string, forkBucketName string) error {
	panic("unimplemented")
}

// PutObject implements [StorageManager].
func (t *tigrisClient) UploadObject(ctx context.Context, bucketName string, key string, file []byte) (resultPath string, err error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(file),
	}
	_, err = t.tigrisStorageClient.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	resultPath = fmt.Sprintf(tigrisBaseURL, bucketName, key)
	return resultPath, nil
}

func (t *tigrisClient) DownloadObject(ctx context.Context, bucketName string, fileName string) (result []byte, err error) {
	resultBytes, err := t.tigrisStorageClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return nil, err
	}
	data, _ := io.ReadAll(resultBytes.Body)
	return data, nil
}

func (t *tigrisClient) ListObject(ctx context.Context, bucketName string) (contents []types.Object, err error) {
	result, err := t.tigrisStorageClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, err
	}
	return result.Contents, nil
}
