package s3feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
)

// fileRunner wraps a runner.IFileRunner and handles S3 download/move lifecycle.
// It implements runner.ITaskRunner so it can be registered in the MuxRunner.
type fileRunner struct {
	reg   *s3client.Registry
	inner runner.IFileRunner
}

func (r *fileRunner) Run(ctx context.Context, item *store.WorkItem) error {
	var payload S3Payload
	if err := decodePayload(item.Payload, &payload); err != nil {
		return fmt.Errorf("s3feed runner: decode payload for %q: %w", item.Id, err)
	}

	svc, ok := r.reg.Get(payload.Service)
	if !ok {
		return fmt.Errorf("s3feed runner: unknown service %q", payload.Service)
	}

	// Download the file. Se il file non c'è più, un tentativo precedente lo ha già processato
	// e spostato (il mark di completamento potrebbe non essere andato a buon fine): trattiamo
	// come già fatto (done) per non ri-processare né fallire. Un download transitorio è invece
	// un RetryError → l'item resta PENDING e viene ritentato, senza creare un item duplicato.
	reader, err := svc.Get(ctx, payload.Key)
	if err != nil {
		if errors.Is(err, s3client.ErrObjectNotFound) {
			log.Info().Msgf("s3feed runner: %q non presente (già spostato), skip", payload.Key)
			return nil
		}
		return store.RetryWithCause(0, fmt.Errorf("s3feed runner: download %q: %w", payload.Key, err))
	}
	defer reader.Close()

	// Delegate to the inner file runner. L'errore del handler segue la convenzione runner
	// (store.ApplyResult): può ritornare store.Retry per essere ritentato, altrimenti va FAILED.
	if err := r.inner.Run(ctx, payload.Key, reader); err != nil {
		log.Warn().Err(err).Msgf("s3feed runner: handler failed for %q", payload.Key)
		return err
	}

	// On success, move the file to the destination path. Un fallimento del move è transitorio:
	// il file (già processato) resta nel sorgente, quindi ritentiamo (RetryError → resta PENDING,
	// stesso WorkItem) invece di marcare FAILED — evita che il feed successivo crei un item
	// duplicato. NB: at-least-once — un move fallito dopo un processamento riuscito comporta un
	// ri-processamento al retry; l'handler IFileRunner deve tollerarlo (idempotenza).
	if payload.DestPath != "" {
		dest := payload.DestPath + "/" + path.Base(payload.Key)
		if err := svc.Move(ctx, payload.Key, dest); err != nil {
			log.Error().Err(err).Msgf("s3feed runner: move %q -> %q failed, retry", payload.Key, dest)
			return store.RetryWithCause(0, fmt.Errorf("s3feed runner: move failed: %w", err))
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
func newFileRunner(reg *s3client.Registry, inner runner.IFileRunner) *fileRunner {
	return &fileRunner{reg: reg, inner: inner}
}
