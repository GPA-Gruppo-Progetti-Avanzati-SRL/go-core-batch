package s3feed

import (
	"context"
	"io"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
)

// objectStore è ciò che feed e fileRunner usano di un servizio S3. È un'interfaccia — soddisfatta da
// *s3client.Service — perché il feed e il ciclo download/spostamento si possano provare con un fake:
// prima i test di questo package non potevano simulare S3, e verificavano path.Match al posto del feed.
type objectStore interface {
	Walk(ctx context.Context, prefix string, visit func(s3client.Object) bool) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Move(ctx context.Context, src, dest string) error
}

// services risolve un servizio S3 logico (chiave di `s3.services`).
type services interface {
	lookup(name string) (objectStore, bool)
}

// registryServices adatta il Registry. Ritorna l'interfaccia solo se il servizio esiste: un
// *Service nil dentro un objectStore non nil passerebbe il controllo di ok.
type registryServices struct{ reg *s3client.Registry }

func (r registryServices) lookup(name string) (objectStore, bool) {
	if r.reg == nil {
		return nil, false
	}
	svc, ok := r.reg.Get(name)
	if !ok {
		return nil, false
	}
	return svc, true
}
