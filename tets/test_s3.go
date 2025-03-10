package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func uploadToYandexS3(localDir, bucketName, s3Folder, accessKey, secretKey string) error {
	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolver(
			aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "https://storage.yandexcloud.net"}, nil
			}),
		),
		config.WithRegion("ru-central1"),
	)
	if err != nil {
		return fmt.Errorf("upload config err AWS: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(s3Client)

	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}

		s3Path := filepath.ToSlash(filepath.Join(s3Folder, relPath))
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		fmt.Printf("Uploading %s to s3://%s/%s\n", path, bucketName, s3Path)
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Path),
			Body:   file,
		})
		return err
	})
}

func main() {
	err := uploadToYandexS3(
		"/path/to/local/files",  
		"your-bucket-name",       
		"backup",                 
		"your-access-key",        
		"your-secret-key",        
	)
	if err != nil {
		log.Fatalf("Loading files err: %v", err)
	}
	fmt.Println("DONE")
}
