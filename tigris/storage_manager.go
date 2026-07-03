package tigris

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/tigrisdata/storage-go"
)

type (
	StorageManagerConfig struct {
		AccessKeyID     string
		SecretAccessKey string
		EndpointURL     string
		Region          string
	}

	StorageManager interface {
		CreateBucket(ctx context.Context, bucketName string) (err error)
		CreateForkBucket(ctx context.Context, bucketName string, forkBucketName string) (err error)
		UploadObject(ctx context.Context, bucketName string, key string, file []byte) (resultPath string, err error)
		DownloadObject(ctx context.Context, bucketName string, fileName string) (result []byte, err error)
		ListObject(ctx context.Context, bucketName string) (contents []types.Object, err error)
	}

	StorageHandler struct {
		config                StorageManagerConfig
		storageManager        StorageManager
		currentStorageManager string
	}

	tigrisClient struct {
		storageManager      StorageManagerConfig
		tigrisStorageClient *storage.Client
	}

	s3Client struct {
		storageManager StorageManagerConfig
		awsS3Client    *s3.Client
	}
)

func (s *StorageHandler) SetStorageManager(storageManager StorageManager) {
	_, ok := storageManager.(*tigrisClient)
	if ok {
		s.currentStorageManager = "TIGRIS"
	}
	_, ok = storageManager.(*s3Client)
	if ok {
		s.currentStorageManager = "S3"
	}
	s.storageManager = storageManager
}

func (s *StorageHandler) GetStorageManager() string {
	return s.currentStorageManager
}

func InitStorageHandler(config StorageManagerConfig) *StorageHandler {
	return &StorageHandler{
		config: config,
	}
}
