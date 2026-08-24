package tigris

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type stubStorage struct {
	createBucketErr error
	uploadPath      string
	uploadErr       error
	download        []byte
	downloadErr     error
	list            []types.Object
	listErr         error
	forkErr         error
}

func (s *stubStorage) CreateBucket(ctx context.Context, bucketName string) error {
	return s.createBucketErr
}

func (s *stubStorage) CreateForkBucket(ctx context.Context, bucketName string, forkBucketName string) error {
	return s.forkErr
}

func (s *stubStorage) UploadObject(ctx context.Context, bucketName string, key string, file []byte) (string, error) {
	return s.uploadPath, s.uploadErr
}

func (s *stubStorage) DownloadObject(ctx context.Context, bucketName string, fileName string) ([]byte, error) {
	return s.download, s.downloadErr
}

func (s *stubStorage) ListObject(ctx context.Context, bucketName string) ([]types.Object, error) {
	return s.list, s.listErr
}

func TestInitStorageHandler(t *testing.T) {
	cfg := StorageManagerConfig{Region: "auto", EndpointURL: "https://example"}
	h := InitStorageHandler(cfg)
	if h == nil {
		t.Fatal("expected handler")
	}
	if h.config != cfg {
		t.Fatalf("config = %+v, want %+v", h.config, cfg)
	}
	if h.GetStorageManager() != "" {
		t.Fatalf("current manager = %q, want empty", h.GetStorageManager())
	}
}

func TestSetStorageManager(t *testing.T) {
	h := InitStorageHandler(StorageManagerConfig{})

	h.SetStorageManager(&tigrisClient{})
	if h.GetStorageManager() != "TIGRIS" {
		t.Fatalf("got %q, want TIGRIS", h.GetStorageManager())
	}

	h.SetStorageManager(&s3Client{})
	if h.GetStorageManager() != "S3" {
		t.Fatalf("got %q, want S3", h.GetStorageManager())
	}

	stub := &stubStorage{}
	h.SetStorageManager(stub)
	if h.storageManager != stub {
		t.Fatal("expected stub to be stored")
	}
	if h.GetStorageManager() != "S3" {
		t.Fatalf("unknown implementation should leave the previous label, got %q", h.GetStorageManager())
	}
}

func TestS3CreateForkBucketNoop(t *testing.T) {
	c := &s3Client{}
	if err := c.CreateForkBucket(context.Background(), "src", "dst"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestObjectURLFormats(t *testing.T) {
	s3URL := fmt.Sprintf(awsS3URLPath, "my-bucket", "us-east-1", "file.txt")
	if s3URL != "https://my-bucket.s3.us-east-1.amazonaws.com/file.txt" {
		t.Fatalf("s3 url = %s", s3URL)
	}
	tigrisURL := fmt.Sprintf(tigrisBaseURL, "my-bucket", "file.txt")
	if tigrisURL != "https://my-bucket.fly.storage.tigris.dev/file.txt" {
		t.Fatalf("tigris url = %s", tigrisURL)
	}
}
