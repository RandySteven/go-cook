package tigris

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	awsS3URLPath = `https://%s.s3.%s.amazonaws.com/%s`
)

func NewS3Client(config StorageManagerConfig) (*s3Client, error) {
	ctx := context.Background()
	awsConfig, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	awsS3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.Region = config.Region
		o.BaseEndpoint = aws.String(config.EndpointURL)
		o.Credentials = aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     config.AccessKeyID,
				SecretAccessKey: config.SecretAccessKey,
				SessionToken:    "",
			}, nil
		}))
	})

	return &s3Client{
		storageManager: config,
		awsS3Client:    awsS3Client,
	}, nil
}

var _ StorageManager = &s3Client{}

// CreateBucket implements [StorageManager].
func (s *s3Client) CreateBucket(ctx context.Context, bucketName string) (err error) {
	_, err = s.awsS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}
	return nil
}

// CreateForkBucket implements [StorageManager].
func (s *s3Client) CreateForkBucket(ctx context.Context, bucketName string, forkBucketName string) (err error) {
	return
}

// UploadObject implements [StorageManager].
func (s *s3Client) UploadObject(ctx context.Context, bucketName string, key string, file []byte) (resultPath string, err error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(file),
	}
	_, err = s.awsS3Client.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	resultPath = fmt.Sprintf(awsS3URLPath, bucketName, s.storageManager.Region, key)
	return resultPath, nil
}

func (s *s3Client) DownloadObject(ctx context.Context, bucketName string, fileName string) (result []byte, err error) {
	resultBytes, err := s.awsS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return nil, err
	}
	data, _ := io.ReadAll(resultBytes.Body)
	return data, nil
}

func (s *s3Client) ListObject(ctx context.Context, bucketName string) (contents []types.Object, err error) {
	result, err := s.awsS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, err
	}
	return result.Contents, nil
}
