package s3client

import (
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Registry holds named S3 Service instances, one per configured service.
type Registry struct {
	services map[string]*Service
}

// NewRegistry creates a Registry from the given Config, initializing an S3 client
// for each entry in Config.Services.
func NewRegistry(cfg *s3.Config) (*Registry, error) {
	if cfg == nil || len(cfg.Services) == 0 {
		return &Registry{services: map[string]*Service{}}, nil
	}
	services := make(map[string]*Service, len(cfg.Services))
	for name, sc := range cfg.Services {
		svc, err := newServiceFromConfig(sc)
		if err != nil {
			return nil, fmt.Errorf("s3 service %q: %w", name, err)
		}
		services[name] = svc
	}
	return &Registry{services: services}, nil
}

// Get returns the Service registered under the given name.
func (r *Registry) Get(name string) (*Service, bool) {
	svc, ok := r.services[name]
	return svc, ok
}

func newServiceFromConfig(sc *s3.ServiceConfig) (*Service, error) {
	region := sc.Region
	if region == "" {
		region = "us-east-1"
	}

	opts := []func(*awss3.Options){
		func(o *awss3.Options) {
			o.Region = region
			o.Credentials = credentials.NewStaticCredentialsProvider(sc.AccessKey, sc.SecretKey, "")
			o.UsePathStyle = sc.UsePathStyle
		},
	}
	if sc.Endpoint != "" {
		opts = append(opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(sc.Endpoint)
		})
	}

	client := awss3.New(awss3.Options{}, opts...)
	return NewService(client, sc.Bucket), nil
}
