// Package s3client contiene l'implementazione dei client S3 del framework batch.
// È un package INTERNAL: importabile solo da codice dentro go-core-batch (s3feed),
// NON dalle applicazioni. I tipi di config pubblici (Config/ServiceConfig) restano
// nel package s3.
package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ErrObjectNotFound è ritornato (wrapped) da Get quando l'oggetto non esiste nel bucket.
// Permette al chiamante di distinguere "file non presente" (es. già spostato da un tentativo
// precedente) da un errore di download transitorio, con errors.Is.
var ErrObjectNotFound = errors.New("s3client: object not found")

// isNotFound riconosce l'errore S3 di oggetto mancante, sia come tipo (*types.NoSuchKey)
// sia come APIError generico con codice NoSuchKey/NotFound (404).
func isNotFound(err error) bool {
	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return true
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// Object represents a single S3 object returned by List.
type Object struct {
	Key  string
	Size int64
}

// Service wraps an S3 client bound to a specific bucket.
type Service struct {
	client *s3.Client
	bucket string
}

// NewService creates a Service from an S3 client and bucket name.
func NewService(client *s3.Client, bucket string) *Service {
	return &Service{client: client, bucket: bucket}
}

// List returns all objects under the given prefix, handling pagination.
func (s *Service) List(ctx context.Context, prefix string) ([]Object, error) {
	var objects []Object
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list prefix=%q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, Object{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			})
		}
	}
	return objects, nil
}

// Get returns a ReadCloser for the object at the given key.
func (s *Service) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("s3 get key=%q: %w", key, ErrObjectNotFound)
		}
		return nil, fmt.Errorf("s3 get key=%q: %w", key, err)
	}
	return out.Body, nil
}

// Move copies src to dest and then deletes src (copy + delete).
func (s *Service) Move(ctx context.Context, src, dest string) error {
	copySource := url.PathEscape(s.bucket + "/" + src)
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(dest),
	})
	if err != nil {
		return fmt.Errorf("s3 copy %q -> %q: %w", src, dest, err)
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(src),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %q after copy: %w", src, err)
	}
	return nil
}
