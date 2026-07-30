package s3client

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestIsNotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"NoSuchKey typed", &types.NoSuchKey{}, true},
		{"APIError NoSuchKey", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"APIError NotFound", &smithy.GenericAPIError{Code: "NotFound"}, true},
		{"APIError altro", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"NoSuchKey wrapped", fmt.Errorf("s3 get: %w", &types.NoSuchKey{}), true},
		{"errore generico", errors.New("boom"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isNotFound(c.err); got != c.want {
				t.Fatalf("isNotFound(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
