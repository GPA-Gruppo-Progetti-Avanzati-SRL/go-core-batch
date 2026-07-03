package s3feed

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
)

// fileRunner wraps a runner.IFileRunner and handles S3 download/move lifecycle.
// It implements runner.ITaskRunner so it can be registered in the MuxRunner.
type fileRunner struct {
	reg   *s3.Registry
	inner runner.IFileRunner
}

func (r *fileRunner) Run(ctx context.Context, objectId string, items store.IWorkItemStore) error {
	// Retrieve the work item to access the S3Payload
	wi, appErr := items.GetById(ctx, objectId)
	if appErr != nil {
		return fmt.Errorf("s3feed runner: get work item %q: %w", objectId, appErr)
	}

	var payload S3Payload
	if err := decodePayload(wi.Payload, &payload); err != nil {
		return fmt.Errorf("s3feed runner: decode payload for %q: %w", objectId, err)
	}

	svc, ok := r.reg.Get(payload.Service)
	if !ok {
		return fmt.Errorf("s3feed runner: unknown service %q", payload.Service)
	}

	// Download the file
	reader, err := svc.Get(ctx, payload.Key)
	if err != nil {
		return fmt.Errorf("s3feed runner: download %q: %w", payload.Key, err)
	}
	defer reader.Close()

	// Delegate to the inner file runner
	if err := r.inner.Run(ctx, payload.Key, reader, items); err != nil {
		log.Warn().Err(err).Msgf("s3feed runner: handler failed for %q, keeping pending", payload.Key)
		return err
	}

	// On success, move the file to the destination path
	if payload.DestPath != "" {
		dest := payload.DestPath + "/" + path.Base(payload.Key)
		if err := svc.Move(ctx, payload.Key, dest); err != nil {
			log.Error().Err(err).Msgf("s3feed runner: move %q -> %q failed", payload.Key, dest)
			return fmt.Errorf("s3feed runner: move failed: %w", err)
		}
		log.Info().Msgf("s3feed runner: moved %q -> %q", payload.Key, dest)
	}

	return nil
}

// decodePayload converts the WorkItem.Payload (any) into an S3Payload.
func decodePayload(raw any, out *S3Payload) error {
	switch v := raw.(type) {
	case S3Payload:
		*out = v
		return nil
	case *S3Payload:
		*out = *v
		return nil
	case map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, out)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("unsupported payload type %T", raw)
		}
		return json.Unmarshal(b, out)
	}
}

// newFileRunner creates a fileRunner that wraps the given IFileRunner with S3 lifecycle.
func newFileRunner(reg *s3.Registry, inner runner.IFileRunner) *fileRunner {
	return &fileRunner{reg: reg, inner: inner}
}
