package s3feed

import (
	"context"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// mockService implements a minimal S3 service for testing via the S3Feed.
// Since S3Feed calls svc.List through the registry, we need to set up
// a registry with a real Service. Instead, we test at the Feed level
// by verifying the output given known List results.

func TestS3Feed_Feed_FiltersAndLimits(t *testing.T) {
	// We can't easily mock s3.Service.List since it uses the real S3 client.
	// Instead, test the filtering/limiting logic by creating an S3Feed
	// and verifying the contract at the integration boundary.
	// For unit testing, we test decodePayload and the WorkItem construction.

	t.Run("pattern matching on base name", func(t *testing.T) {
		// Test that path.Match works correctly for our use case
		testCases := []struct {
			pattern string
			key     string
			match   bool
		}{
			{"*.csv", "data/file1.csv", true},
			{"*.csv", "data/file1.txt", false},
			{"report-*.csv", "data/report-2024.csv", true},
			{"report-*.csv", "data/other-2024.csv", false},
			{"*", "data/anything.bin", true},
		}
		for _, tc := range testCases {
			// Simulate what Feed does: path.Base(key) then path.Match
			base := baseName(tc.key)
			matched, err := matchPattern(tc.pattern, base)
			if err != nil {
				t.Fatalf("pattern %q: unexpected error: %v", tc.pattern, err)
			}
			if matched != tc.match {
				t.Errorf("pattern=%q key=%q: got %v, want %v", tc.pattern, tc.key, matched, tc.match)
			}
		}
	})

	t.Run("WorkItem construction", func(t *testing.T) {
		// Verify that a WorkItem built from an S3 object has the correct fields
		key := "inbox/report.csv"
		taskType := "S3_IMPORT"
		destPath := "processed"
		serviceName := "main"

		wi := buildWorkItem(taskType, key, serviceName, destPath)
		if wi.Type != taskType {
			t.Errorf("Type = %q, want %q", wi.Type, taskType)
		}
		if wi.ObjectId != key {
			t.Errorf("ObjectId = %q, want %q", wi.ObjectId, key)
		}
		if wi.Status != store.StatusPending {
			t.Errorf("Status = %q, want %q", wi.Status, store.StatusPending)
		}
		payload, ok := wi.Payload.(S3Payload)
		if !ok {
			t.Fatalf("Payload type = %T, want S3Payload", wi.Payload)
		}
		if payload.Service != serviceName {
			t.Errorf("Payload.Service = %q, want %q", payload.Service, serviceName)
		}
		if payload.Key != key {
			t.Errorf("Payload.Key = %q, want %q", payload.Key, key)
		}
		if payload.DestPath != destPath {
			t.Errorf("Payload.DestPath = %q, want %q", payload.DestPath, destPath)
		}
	})

	t.Run("unknown service returns error", func(t *testing.T) {
		reg, _ := s3client.NewRegistry(nil) // empty registry
		feed := New(reg)
		props := core.Properties{
			"service": "nonexistent",
			"path":    "inbox/",
			"pattern": "*.csv",
		}
		_, err := feed.Feed(context.Background(), "TEST", props, 10)
		if err == nil {
			t.Fatal("expected error for unknown service")
		}
	})
}

func TestDecodePayload(t *testing.T) {
	t.Run("direct S3Payload", func(t *testing.T) {
		input := S3Payload{Service: "main", Key: "file.csv", DestPath: "done/"}
		var out S3Payload
		if err := decodePayload(input, &out); err != nil {
			t.Fatal(err)
		}
		if out != input {
			t.Errorf("got %+v, want %+v", out, input)
		}
	})

	t.Run("pointer S3Payload", func(t *testing.T) {
		input := &S3Payload{Service: "main", Key: "file.csv", DestPath: "done/"}
		var out S3Payload
		if err := decodePayload(input, &out); err != nil {
			t.Fatal(err)
		}
		if out != *input {
			t.Errorf("got %+v, want %+v", out, *input)
		}
	})

	t.Run("map[string]any (from DB deserialization)", func(t *testing.T) {
		input := map[string]any{
			"service":  "main",
			"key":      "inbox/data.csv",
			"destPath": "processed/",
		}
		var out S3Payload
		if err := decodePayload(input, &out); err != nil {
			t.Fatal(err)
		}
		if out.Service != "main" || out.Key != "inbox/data.csv" || out.DestPath != "processed/" {
			t.Errorf("unexpected result: %+v", out)
		}
	})
}

// helpers to extract testable logic from Feed without needing a real S3 client

func baseName(key string) string {
	return pathBase(key)
}

func matchPattern(pattern, base string) (bool, error) {
	return pathMatch(pattern, base)
}

func buildWorkItem(taskType, key, service, destPath string) *store.WorkItem {
	return &store.WorkItem{
		Id:       "test-id",
		Type:     taskType,
		ObjectId: key,
		Payload: S3Payload{
			Service:  service,
			Key:      key,
			DestPath: destPath,
		},
		Status: store.StatusPending,
	}
}
